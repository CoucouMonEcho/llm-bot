// Package bot 提供最顶层的"消息循环管道"——把 Adapter 的接收
// 事件灌进 Agent Runnable，把结果回发到 Adapter。
//
// 这个包刻意做得很薄。它不应该承载任何业务判断（那些都在 Agent Graph 中），
// 只管并发调度与错误可观测。
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
	"github.com/echo/llm-bot/internal/domain"
)

// maxConcurrent 单进程同时处理的最大消息数。
// 这个值限制了 Redis / LLM 的并发压力；超出时新消息会排队等待通道消费。
const maxConcurrent = 32

// replyTimeout 单条消息从触发到发送完成的最大时长。
// 超时意味着整个链路（防护、主链、后处理、落历史、发送）都断掉。
const replyTimeout = 60 * time.Second

const sendDeadlineReserve = 300 * time.Millisecond

type replyLengthTier int

const (
	replyTierShort replyLengthTier = iota
	replyTierMedium
	replyTierLong
)

// Bot 聚合 Adapter 与 Runnable，是整个服务的"大主循环"载体。
type Bot struct {
	adapter  adapter.Adapter
	runnable agent.Runnable
	logger   *slog.Logger

	activityRecorder ActivityRecorder

	sem chan struct{} // 并发信号量
}

// ActivityRecorder 是 Bot 写入主动消息候选索引所需的最小接口。
//
// Bot 只知道"真实入站消息刚发生"，不应该知道主动消息如何按好感度排序、
// 如何写 Redis key、如何读运行期开关。因此这里收敛成一个无返回值接口：
// 实现侧负责软降级和日志，记录失败不能中断主回复链路，也不能迫使 bot 包
// 反向依赖 proactive 包。
type ActivityRecorder interface {
	// RecordInbound 记录一条真实入站消息，用于后续主动消息候选选择、冷却判断和短上下文拼接。
	RecordInbound(ctx context.Context, msg *domain.InboundMessage)
}

// New 构造一个 Bot。
//
// ad 可以是任何满足 adapter.Adapter 的实现；rn 是 agent.Build 的返回值。
// stats 打分由 Agent Graph 的 scoreStats 节点在"回复已生成"时触发，Bot 只负责发送。
func New(ad adapter.Adapter, rn agent.Runnable, recorder ActivityRecorder, logger *slog.Logger) *Bot {
	return &Bot{
		adapter:          ad,
		runnable:         rn,
		logger:           logger.With(slog.String("component", "bot")),
		activityRecorder: recorder,
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
	receivedAt := time.Now()
	ctx, cancel := context.WithTimeout(parent, replyTimeout)
	defer cancel()

	lg := b.logger.With(
		slog.String("session", m.SessionID),
		slog.String("user", m.UserID))

	if b.activityRecorder != nil {
		// 先记录活跃再进 Graph：即便后续 LLM 调用失败，"这个人刚来过"仍是事实。
		// 记录接口不返回错误，主动消息索引故障只在实现侧降级。
		b.activityRecorder.RecordInbound(ctx, m)
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

	tier := classifyReplyLength(state.Reply.Content)
	if err := waitBeforeSend(ctx, receivedAt, targetReplyLatency(tier)); err != nil {
		lg.Warn("reply delayed until context done", slog.Any("err", err))
		return
	}

	// 步骤 3：回发。
	out := &domain.OutboundMessage{
		Platform:  m.Platform,
		ConvType:  m.ConvType,
		SessionID: m.SessionID,
		Text:      state.Reply.Content,
		ReplyTo:   decideReplyTarget(m, tier),
	}
	if err := b.adapter.Send(ctx, out); err != nil {
		lg.Error("adapter send failed", slog.Any("err", err))
		return
	}

	// 步骤 4：观测日志——被拦截的路径走 info 以便线上报警，正常路径降到 debug
	// 避免刷屏。
	if state.Verdict.Blocked() {
		lg.Info("reply sent (blocked path)",
			slog.String("verdict", state.Verdict.String()),
			slog.String("detail", state.Verdict.Detail))
	} else {
		lg.Debug("reply sent",
			slog.Int("len", len(state.Reply.Content)))
	}

}

// classifyReplyLength 用 rune 数而不是字节数衡量文本量，中文和 emoji 都能稳定分档。
func classifyReplyLength(text string) replyLengthTier {
	n := utf8.RuneCountInString(strings.TrimSpace(text))
	switch {
	case n <= 12:
		return replyTierShort
	case n <= 35:
		return replyTierMedium
	default:
		return replyTierLong
	}
}

// targetReplyLatency 是从收到消息到发出回复的目标总耗时。
//
// 这些数值比常见移动端/中文输入速度更快，但能避免 LLM 生成很快时出现"秒回长文"
// 的机器感。若模型调用本身已经超过目标耗时，waitBeforeSend 不会再额外等待。
func targetReplyLatency(tier replyLengthTier) time.Duration {
	switch tier {
	case replyTierShort:
		return 1200 * time.Millisecond
	case replyTierMedium:
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
// 当前策略：
//   - 私聊：无需指向，返回 nil；
//   - 群聊短回复：直接发送，不 @，降低轻量回复的打扰感；
//   - 群聊中等回复：@ 发信人；
//   - 群聊长回复：优先引用原消息，缺少 MessageID 时降级为 @。
func decideReplyTarget(m *domain.InboundMessage, tier replyLengthTier) *domain.ReplyTarget {
	if m == nil || m.ConvType != domain.ConversationGroup {
		return nil
	}
	if tier == replyTierShort {
		return nil
	}
	if tier == replyTierLong && m.MessageID != "" {
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
