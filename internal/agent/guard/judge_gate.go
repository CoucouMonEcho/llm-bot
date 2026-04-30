// Package guard 的 judge_gate.go 实现 Graph 起点处的 LLM 裁判节点。
//
// 位置：START → judgeGate → loadContext。
// judgeGate 负责把 flow.Input 装箱成 flow.State，并做输入侧安全判定；只有
// 裁判明确输出 safe 时才放行，其它情况直接返回 flow.ErrSkipReply，由 Bot 静默不回复。
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
// judge 为 nil 或调用失败时都 fail-closed。删掉正则层后，裁判是唯一输入侧
// 安全判定；因此不能再因为裁判不可用而默认放行。
func NewJudgeGate(judge *Judge, logger *slog.Logger) *compose.Lambda {
	lg := cmp.Or(logger, slog.Default()).With(slog.String("node", "judgeGate"))
	return compose.InvokableLambda(func(ctx context.Context, in *flow.Input) (*flow.State, error) {
		st := flow.NewState(in)
		session := ""
		query := ""
		if in != nil {
			session = in.SessionID
			query = in.Query
		}

		if judge == nil {
			st.VerdictKind = flow.VerdictJudge
			lg.Warn("judge missing, fail-closed", slog.String("session", session))
			return nil, flow.ErrSkipReply
		}

		safe, err := judge.Classify(ctx, query)
		if err != nil {
			st.VerdictKind = flow.VerdictJudge
			lg.Warn("judge error, fail-closed",
				slog.String("session", session),
				slog.Any("err", err))
			return nil, flow.ErrSkipReply
		}
		if !safe {
			st.VerdictKind = flow.VerdictJudge
			lg.Info("judge blocked", slog.String("session", session))
			return nil, flow.ErrSkipReply
		}
		return st, nil
	})
}
