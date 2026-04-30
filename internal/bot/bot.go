// Package bot 提供最顶层的"消息循环管道"——把 Adapter 的接收
// 事件灌进 Agent Runnable，把结果回发到 Adapter。
//
// 这个包刻意做得很薄。除入站触发 gate 这种进入 Graph 前的流量控制外，
// 业务判断都应留在 Agent Graph 中；这里主要负责并发调度与错误可观测。
package bot

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/echo/llm-bot/internal/adapter"
	"github.com/echo/llm-bot/internal/agent"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/domain"
	"github.com/echo/llm-bot/internal/proactive"
	"github.com/echo/llm-bot/internal/store"
)

// maxConcurrent 单进程同时处理的最大消息数。
// 这个值限制了 Redis / LLM 的并发压力；超出时新消息会排队等待通道消费。
const maxConcurrent = 32

// replyTimeout 单条消息从触发到发送完成的最大时长。
// 超时意味着整个链路（防护、主链、后处理、落历史、发送）都断掉。
const replyTimeout = 60 * time.Second

const sendDeadlineReserve = 300 * time.Millisecond

// Bot 聚合 Adapter 与 Runnable，是整个服务的"大主循环"载体。
//
// activityRecorder 直接持有 *proactive.ActivityRecorder：由 main 装配，恒非
// nil；这条旁路记录的具体写入行为（Redis key、白名单、软降级）都收敛在
// proactive 包里。
type Bot struct {
	adapter  adapter.Adapter
	runnable agent.Runnable
	logger   *slog.Logger

	activityRecorder *proactive.ActivityRecorder

	// groupBuffer 是"群聊短期上下文"的写入端，可为 nil。
	// 为 nil 时表示该功能在 main 装配阶段被关闭，Bot 完全不调用，
	// 等价于关闭群聊背景注入；非 nil 时仅在被 follow-up gate 决定
	// 不进 Graph 的群聊普通消息上做 Append（详见 cacheGroupBackground）。
	groupBuffer store.GroupBufferRepo

	followupWindow time.Duration
	followups      map[followupKey]time.Time
	followupsMu    sync.Mutex

	now func() time.Time
	sem chan struct{} // 并发信号量
}

type followupKey struct {
	sessionID string
	userID    string
}

// New 构造一个 Bot。
//
// ad 可以是任何满足 adapter.Adapter 的实现；rn 是 agent.Build 的返回值。
// recorder 由 main 装配，恒非 nil；运行期是否真的发主动消息由 Redis
// `bot_proactive_enabled` 决定。stats 打分由 Agent Graph 的 scoreStats
// 节点在"回复已生成"时触发，Bot 只负责发送。
//
// groupBuffer 可为 nil：为 nil 时 Bot 完全不调用，等价于关闭"群聊短期
// 上下文缓存"；非 nil 时仅在 follow-up gate 拦下的群聊普通消息上做一次
// 旁路 Append——具体见 cacheGroupBackground 注释。
func New(ad adapter.Adapter, rn agent.Runnable, recorder *proactive.ActivityRecorder, groupBuffer store.GroupBufferRepo, logger *slog.Logger, trigger config.Trigger) *Bot {
	followupWindow := time.Duration(trigger.GroupFollowupSec) * time.Second
	if followupWindow < 0 {
		followupWindow = 0
	}
	return &Bot{
		adapter:          ad,
		runnable:         rn,
		logger:           logger.With(slog.String("component", "bot")),
		activityRecorder: recorder,
		groupBuffer:      groupBuffer,
		followupWindow:   followupWindow,
		followups:        make(map[followupKey]time.Time),
		now:              time.Now,
		sem:              make(chan struct{}, maxConcurrent),
	}
}

// Run 启动主循环，阻塞直到 ctx 取消或 Adapter 的接收通道关闭。
//
// 每收到一条消息，用信号量限流后交给独立的 goroutine 处理，
// 避免单条慢请求阻塞后续消息。
func (b *Bot) Run(ctx context.Context) {
	var wg sync.WaitGroup

	b.logger.Info("bot loop started")
	defer func() {
		// 等所有正在处理的 handle goroutine 收尾，避免进程退出时丢消息。
		wg.Wait()
		b.logger.Info("bot loop exited")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-b.adapter.Receive():
			if !ok {
				return
			}
			// 获取信号量；若达到上限会阻塞，这是有意的反压。
			select {
			case b.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Add(1)
			go func(m *domain.InboundMessage) {
				defer wg.Done()
				defer func() { <-b.sem }()
				b.handle(ctx, m)
			}(msg)
		}
	}
}

// handle 处理单条消息的完整生命周期：
//  1. 把 InboundMessage 翻译成 flow.Input；
//  2. 调用 Runnable.Invoke，得到 flow.State；
//  3. 把 state.Reply 包成 OutboundMessage 回发；
//  4. 全程带 replyTimeout 以保证慢请求不会泄漏协程。
//
// stats 打分不在这里做：Agent Graph 已在回复生成后通过 scoreStats 异步触发，
// 不再把参数更新绑定到 Adapter 发送成功与否。
func (b *Bot) handle(parent context.Context, m *domain.InboundMessage) {
	receivedAt := b.currentTime()
	ctx, cancel := context.WithTimeout(parent, replyTimeout)
	defer cancel()

	lg := b.logger.With(
		slog.String("session", m.SessionID),
		slog.String("user", m.UserID))

	// 先记录活跃再进 Graph：即便后续 LLM 调用失败，"这个人刚来过"仍是事实。
	// 记录接口不返回错误，主动消息索引故障只在实现侧降级。
	b.activityRecorder.RecordInbound(ctx, m)

	if !b.shouldInvokeGraph(m, receivedAt) {
		b.cacheGroupBackground(ctx, m)
		lg.Debug("message skipped by follow-up gate")
		return
	}

	// 步骤 1：构造 Graph 入参。
	// UserID 透传给 Graph：stats 按人头维度读写（好感度的 ZSET member 形如
	// "<platform>_<userID>"），群聊里一个 SessionID 对应多个 UserID，
	// 不能用 SessionID 顶替；Platform 用来在跨平台场景里隔离同号用户。
	in := &flow.Input{
		Platform:  string(m.Platform),
		SessionID: m.SessionID,
		ConvType:  string(m.ConvType),
		UserID:    m.UserID,
		Query:     m.Text,
	}

	// 步骤 2：驱动 Graph。
	state, err := b.runnable.Invoke(ctx, in)
	if err != nil {
		lg.Error("agent invoke failed", slog.Any("err", err))
		return
	}
	if state == nil || state.Reply == nil || state.Reply.Content == "" {
		lg.Warn("agent produced empty reply")
		return
	}

	runeCount := utf8.RuneCountInString(strings.TrimSpace(state.Reply.Content))
	if err := waitBeforeSend(ctx, receivedAt, targetReplyLatency(runeCount)); err != nil {
		lg.Warn("reply delayed until context done", slog.Any("err", err))
		return
	}

	// 步骤 3：回发。
	out := &domain.OutboundMessage{
		Platform:  m.Platform,
		ConvType:  m.ConvType,
		SessionID: m.SessionID,
		Text:      state.Reply.Content,
		ReplyTo:   decideReplyTarget(m, runeCount),
	}
	if err := b.adapter.Send(ctx, out); err != nil {
		lg.Error("adapter send failed", slog.Any("err", err))
		return
	}
	b.refreshFollowup(m, b.currentTime())

	// 步骤 4：观测日志——被拦截的路径走 info 以便线上报警，正常路径降到 debug
	// 避免刷屏。被拦截的具体细节（命中的 pattern、裁判输出）由产生它的节点
	// 当场打日志，这里只标记"走的是哪一类拦截"。
	if state.VerdictKind != flow.VerdictSafe {
		lg.Info("reply sent (blocked path)",
			slog.String("verdict", state.VerdictKind.String()))
	} else {
		lg.Debug("reply sent",
			slog.Int("len", len(state.Reply.Content)))
	}

}

func (b *Bot) currentTime() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

func (b *Bot) shouldInvokeGraph(m *domain.InboundMessage, now time.Time) bool {
	if m.ConvType != domain.ConversationGroup {
		return true
	}
	if m.ExplicitTrigger {
		return true
	}
	if b.followupWindow <= 0 || m.SessionID == "" || m.UserID == "" {
		return false
	}

	key := followupKey{sessionID: m.SessionID, userID: m.UserID}
	b.followupsMu.Lock()
	defer b.followupsMu.Unlock()

	expiresAt, ok := b.followups[key]
	if !ok {
		return false
	}
	if now.Before(expiresAt) {
		return true
	}
	delete(b.followups, key)
	return false
}

// cacheGroupBackground 把一条"被 follow-up gate 决定不进 Graph 的群聊普通消息"
// 旁路写入群聊短期上下文缓存。
//
// 调用约束：本方法只在 handle 中"不进 Graph"的分支被调用，目的是让
// groupBuffer 里只保留 bot 当时没有正式回应、也不会写进 history 的群聊
// 普通消息——避免与 saveHistory 写入的对话历史在同一窗口内重复。
//
// 失败语义：写入失败仅记一条 debug 日志，不阻断主流程。这条消息本来就
// 已经被 gate 丢弃，缓存写不下去也不会让用户感知到任何"丢回复"，完全
// 可降级。groupBuffer == nil 时整段功能关闭，直接返回。
func (b *Bot) cacheGroupBackground(ctx context.Context, m *domain.InboundMessage) {
	if b.groupBuffer == nil || m == nil {
		return
	}
	if m.ConvType != domain.ConversationGroup {
		return
	}
	if m.SessionID == "" || m.UserID == "" {
		return
	}
	if strings.TrimSpace(m.Text) == "" {
		return
	}
	if err := b.groupBuffer.Append(ctx, m.SessionID, m.UserID, m.UserName, m.Text); err != nil {
		b.logger.Debug("group buffer append failed",
			slog.String("session", m.SessionID),
			slog.String("user", m.UserID),
			slog.Any("err", err))
	}
}

func (b *Bot) refreshFollowup(m *domain.InboundMessage, now time.Time) {
	if m.ConvType != domain.ConversationGroup || b.followupWindow <= 0 || m.SessionID == "" || m.UserID == "" {
		return
	}

	key := followupKey{sessionID: m.SessionID, userID: m.UserID}
	b.followupsMu.Lock()
	defer b.followupsMu.Unlock()

	for k, expiresAt := range b.followups {
		if !now.Before(expiresAt) {
			delete(b.followups, k)
		}
	}
	b.followups[key] = now.Add(b.followupWindow)
}

// targetReplyLatency 是从收到消息到发出回复的目标总耗时。
//
// 用 rune 数（而非字节数）衡量文本量，中文和 emoji 都能稳定分档。两个阈值
// 12 / 35 划分短 / 中 / 长三档；这些数值比常见移动端/中文输入速度更快，但能
// 避免 LLM 生成很快时出现"秒回长文"的机器感。若模型调用本身已经超过目标
// 耗时，waitBeforeSend 不会再额外等待。
func targetReplyLatency(runeCount int) time.Duration {
	switch {
	case runeCount <= 12:
		return 1200 * time.Millisecond
	case runeCount <= 35:
		return 3500 * time.Millisecond
	default:
		return 6500 * time.Millisecond
	}
}

func waitBeforeSend(ctx context.Context, receivedAt time.Time, targetTotal time.Duration) error {
	if targetTotal <= 0 {
		return nil
	}
	delay := targetTotal - time.Since(receivedAt)
	if delay <= 0 {
		return nil
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) - sendDeadlineReserve
		if remaining <= 0 {
			return nil
		}
		if delay > remaining {
			delay = remaining
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// decideReplyTarget 逐条决定本次回复要不要 @、要不要引用、或是什么都不加。
//
// 这是 Bot 层"回复样式"唯一的决策入口。之所以写成一个函数而不是挂到
// 全局 config / Bot 结构上，是因为"样式"天然属于消息级语境——
//   - "主动发消息、不艾特任何人"       → 返回 nil；
//   - "短句不打扰、中句 @、长句引用"    → 根据回复文本量分支；
//   - "被攻击的降级回复不引用原消息"   → 未来可再传入 *flow.State 判断。
//
// 任何新增策略都应当只改本函数的实现，避免把开关在全局配置里四处蔓延。
//
// 当前策略（按 rune 数 12 / 35 划分长度档位）：
//   - 私聊：无需指向，返回 nil；
//   - 群聊短回复（≤12）：直接发送，不 @，降低轻量回复的打扰感；
//   - 群聊中等回复：@ 发信人；
//   - 群聊长回复（>35）：优先引用原消息，缺少 MessageID 时降级为 @。
func decideReplyTarget(m *domain.InboundMessage, runeCount int) *domain.ReplyTarget {
	if m == nil || m.ConvType != domain.ConversationGroup {
		return nil
	}
	if runeCount <= 12 {
		return nil
	}
	if runeCount > 35 && m.MessageID != "" {
		return &domain.ReplyTarget{
			Mode:      domain.ReplyModeQuote,
			MessageID: m.MessageID,
		}
	}
	if m.UserID == "" {
		return nil
	}
	return &domain.ReplyTarget{
		Mode:   domain.ReplyModeAt,
		UserID: m.UserID,
	}
}
