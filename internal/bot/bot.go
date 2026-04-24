// Package bot 提供最顶层的"消息循环 plumbing"——把 Adapter 的接收
// 事件灌进 Agent Runnable，把结果回发到 Adapter。
//
// 这个包刻意做得很薄。它不应该承载任何业务判断（那些都在 Agent Graph 中），
// 只管并发调度与错误可观测。
package bot

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/echo/llm-bot/internal/adapter"
	"github.com/echo/llm-bot/internal/agent"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/domain"
)

// maxConcurrent 单进程同时处理的最大消息数。
// 这个值限制了 Redis / LLM 的并发压力；超出时新消息会排队等待 channel 消费。
const maxConcurrent = 32

// replyTimeout 单条消息从触发到发送完成的最大时长。
// 超时意味着整个链路（guard + 主链 + postproc + saveHistory + 发送）都断掉。
const replyTimeout = 60 * time.Second

// Bot 聚合 Adapter 与 Runnable，是整个服务的"大主循环"载体。
type Bot struct {
	adapter  adapter.Adapter
	runnable agent.Runnable
	logger   *slog.Logger

	sem chan struct{} // 并发信号量
}

// New 构造一个 Bot。
//
// ad 可以是任何满足 adapter.Adapter 的实现；rn 是 agent.Build 的返回值。
func New(ad adapter.Adapter, rn agent.Runnable, logger *slog.Logger) *Bot {
	return &Bot{
		adapter:  ad,
		runnable: rn,
		logger:   logger.With(slog.String("component", "bot")),
		sem:      make(chan struct{}, maxConcurrent),
	}
}

// Run 启动主循环，阻塞直到 ctx 取消或 Adapter 的 Receive channel 关闭。
//
// 每收到一条消息，用 semaphore 限流后交给独立的 goroutine 处理，
// 避免单条慢请求阻塞后续消息。
func (b *Bot) Run(ctx context.Context) {
	var wg sync.WaitGroup

	b.logger.Info("bot loop started")
	defer func() {
		// 等所有 in-flight handle goroutine 收尾，避免进程退出时丢消息。
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
func (b *Bot) handle(parent context.Context, m *domain.InboundMessage) {
	ctx, cancel := context.WithTimeout(parent, replyTimeout)
	defer cancel()

	lg := b.logger.With(
		slog.String("session", m.SessionID),
		slog.String("user", m.UserID))

	// Step 1: 构造 Graph 入参
	in := &flow.Input{
		SessionID: m.SessionID,
		Query:     m.Text,
		UserName:  m.UserName,
	}

	// Step 2: 驱动 Graph
	state, err := b.runnable.Invoke(ctx, in)
	if err != nil {
		lg.Error("agent invoke failed", slog.Any("err", err))
		return
	}
	if state == nil || state.Reply == nil || state.Reply.Content == "" {
		lg.Warn("agent produced empty reply")
		return
	}

	// Step 3: 回发
	out := &domain.OutboundMessage{
		Platform:  m.Platform,
		ConvType:  m.ConvType,
		SessionID: m.SessionID,
		Text:      state.Reply.Content,
		ReplyTo:   decideReplyTarget(m),
	}
	if err := b.adapter.Send(ctx, out); err != nil {
		lg.Error("adapter send failed", slog.Any("err", err))
		return
	}

	// Step 4: 观测日志——被拦截的路径走 info 以便线上报警，正常路径降到 debug
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

// decideReplyTarget 逐条决定本次回复要不要 @、要不要引用、或是什么都不加。
//
// 这是 Bot 层"回复样式"唯一的决策入口。之所以写成一个函数而不是挂到
// 全局 config / Bot 结构上，是因为"样式"天然属于消息级语境——
//   - "主动发消息、不艾特任何人"       → 返回 nil；
//   - "某些场景艾特、某些场景不艾特"   → 根据 m 的上下文分支；
//   - "被攻击的降级回复不引用原消息"   → 未来可再传入 *flow.State 判断。
//
// 任何新增策略都应当只改本函数的实现，避免把开关在全局配置里四处蔓延。
//
// 当前默认策略（在更细的规则敲定前，先保证行为可用）：
//   - 私聊：无需指向，返回 nil；
//   - 群聊且已知发信人：@ 发信人，让群友看清楚是回给谁的；
//   - 其他不满足条件的情况（比如缺失 UserID）：返回 nil，Adapter 按纯文本发送。
func decideReplyTarget(m *domain.InboundMessage) *domain.ReplyTarget {
	if m == nil || m.ConvType != domain.ConversationGroup {
		return nil
	}
	if m.UserID == "" {
		return nil
	}
	return &domain.ReplyTarget{
		Mode:   domain.ReplyModeAt,
		UserID: m.UserID,
	}
}
