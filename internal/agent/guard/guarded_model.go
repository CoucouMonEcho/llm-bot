// Package guard 的 guarded_model.go 实现"主模型 Generate + LLM 裁判并行"的节点。
//
// 位置：buildMessages → guardedModel → (branch by VerdictKind)
//   - VerdictKind 仍为 Safe → 走 postproc → saveHistory → scoreStats → END；
//   - VerdictKind 被裁判置为 VerdictJudge → 走 fallback → scoreStats → END。
//
// 为什么把"主模型调用 + 裁判并行"留在同一个节点而不是再拆两个：
//   - eino Graph 原生 fan-out/fan-in 无法实现"兄弟分支中断"——只有我们自己
//     掌管 goroutine 生命周期时才能做到"裁判判定攻击 → 立刻 cancel 主链"，
//     尽早断开主模型的 HTTP 请求以节省 token；
//   - 把这个并发黑盒聚合在一个节点里反而是更诚实的抽象：节点的外观仍是
//     "Messages → Reply"，并发与 cancel 是实现细节，不污染 Graph 拓扑。
//
// 本节点的输入已是 state.Messages（buildMessages 组装好的完整消息列表），
// 产出 state.Reply 或把 state.VerdictKind 设为 VerdictJudge。Persona / History
// 的组装在上游节点完成，本节点只消费 Messages——职责边界清晰，方便单测。
package guard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"golang.org/x/sync/errgroup"
)

// NewGuardedModel 构造 guardedModel 节点。
//
// mainModel 为主对话模型；judge 为 nil 时节点退化为"纯串行 Generate"。
// logger 必须非 nil（启动期由 agent.Build 注入）。
func NewGuardedModel(mainModel model.BaseChatModel, judge *Judge, logger *slog.Logger) *compose.Lambda {
	lg := logger.With(slog.String("node", "guardedModel"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		// -----------------------------------------------------------
		// 裁判未启用：退化为纯串行，直接调用主模型后返回。
		// 这条快速路径让"关闭裁判"的部署无需任何并发开销。
		// -----------------------------------------------------------
		if judge == nil {
			reply, err := mainModel.Generate(ctx, st.Messages)
			if err != nil {
				return nil, fmt.Errorf("agent: main chain: %w", err)
			}
			st.Reply = reply
			return st, nil
		}

		// -----------------------------------------------------------
		// 并行执行"主链" 与 "LLM 裁判"。
		//
		// 设计要点：
		//  - subCtx 从外层 ctx 派生；defer cancel() 兜底释放。
		//  - 两个 goroutine 共享 subCtx：
		//      * 主链调用 Generate 时把 subCtx 传下去，OpenAI 兼容客户端
		//        会在 ctx.Done() 时断开 HTTP 请求，停止继续消耗 token；
		//      * 裁判一旦判定 attack，立刻主动 cancel 来终止主链。
		// -----------------------------------------------------------
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var (
			mainReply *schema.Message
			attack    bool
		)

		eg, egCtx := errgroup.WithContext(subCtx)

		// -- 主链 goroutine --
		eg.Go(func() error {
			r, err := mainModel.Generate(egCtx, st.Messages)
			if err != nil {
				// 如果是被主动 cancel（因检测出攻击），归一化为 context.Canceled，
				// 让外层统一识别。
				if errors.Is(err, context.Canceled) {
					return context.Canceled
				}
				return fmt.Errorf("agent: main chain: %w", err)
			}
			mainReply = r
			return nil
		})

		// -- 裁判 goroutine --
		eg.Go(func() error {
			isAttack, err := judge.Classify(egCtx, st.In.Query)
			if err != nil {
				// 裁判失败（网络/超时）不传递错误，只打日志——
				// 保守地放行，由其他防线兜底。
				if errors.Is(err, context.Canceled) {
					return nil
				}
				lg.Warn("judge error, fail-open",
					slog.String("session", st.In.SessionID),
					slog.Any("err", err))
				return nil
			}
			if isAttack {
				attack = true
				// 关键一步：立即 cancel，让主链的 HTTP 请求尽快断开。
				cancel()
			}
			return nil
		})

		// 等待两个分支都结束。Wait 返回第一个非 nil 错误；
		// 因此"主链被 cancel → 返回 context.Canceled"是预期内的，需要吞掉。
		if err := eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			return nil, err
		}

		// -----------------------------------------------------------
		// 按裁判结论产出 State。
		// -----------------------------------------------------------
		if attack {
			st.VerdictKind = flow.VerdictJudge
			lg.Info("judge blocked", slog.String("session", st.In.SessionID))
			return st, nil
		}

		// 理论上：未被拦截时 mainReply 必非 nil（主链成功返回）。
		// 若 mainReply 为 nil，说明主链在 cancel 未触发的情况下返回了空——
		// 这是错误情况，返回错误让上层走 Bot 的错误路径（不回复 + 日志）。
		if mainReply == nil {
			return nil, fmt.Errorf("agent: main chain returned nil reply without error")
		}
		st.Reply = mainReply
		return st, nil
	})
}
