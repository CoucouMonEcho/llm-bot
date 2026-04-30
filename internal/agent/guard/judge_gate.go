// Package guard 的 judge_gate.go 实现 Graph 中显式的 LLM 裁判前置节点。
//
// 位置：regexGate(放行) → judgeGate → loadContext。
// judgeGate 只负责输入侧安全判定，不调用主聊天模型；判定为 attack 时写入
// flow.VerdictJudge，由 Graph branch 路由到 fallback。
package guard

import (
	"cmp"
	"context"
	"log/slog"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
)

// NewJudgeGate 构造 judgeGate 节点。
//
// judge 为 nil 时节点退化为 no-op。裁判调用失败时 fail-open：只记录 warn，
// 保持 VerdictKind 不变，让后续主链继续生成回复。
func NewJudgeGate(judge *Judge, logger *slog.Logger) *compose.Lambda {
	lg := cmp.Or(logger, slog.Default()).With(slog.String("node", "judgeGate"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		if judge == nil {
			return st, nil
		}

		attack, err := judge.Classify(ctx, st.In.Query)
		if err != nil {
			lg.Warn("judge error, fail-open",
				slog.String("session", st.In.SessionID),
				slog.Any("err", err))
			return st, nil
		}
		if attack {
			st.VerdictKind = flow.VerdictJudge
			lg.Info("judge blocked", slog.String("session", st.In.SessionID))
		}
		return st, nil
	})
}
