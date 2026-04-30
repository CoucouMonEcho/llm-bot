// Package nodes 的 judge_gate.go 实现 Graph 起点处的 LLM 裁判节点。
//
// 位置：START → judgeGate → loadContext。
// judgeGate 负责把 flow.Input 装箱成 flow.State，并做输入侧安全判定；只有
// 裁判明确输出 safe 时才放行，其它情况直接返回 flow.ErrSkipReply，由 Bot 静默不回复。
package nodes

import (
	"cmp"
	"context"
	"log/slog"
	"strings"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
)

const (
	judgeTokenSafe   = "safe"
	judgeTokenAttack = "attack"
)

// NewJudgeGate 构造 judgeGate 节点。
//
// judgeModel 为 nil、prompt 为空或调用失败时都按不放行处理。删掉正则层后，
// 裁判是唯一输入侧安全判定；因此不能再因为裁判不可用而默认放行。
func NewJudgeGate(judgeModel model.BaseChatModel, systemPrompt string, logger *slog.Logger) *compose.Lambda {
	lg := cmp.Or(logger, slog.Default()).With(slog.String("node", "judgeGate"))
	return compose.InvokableLambda(func(ctx context.Context, in *flow.Input) (*flow.State, error) {
		st := flow.NewState(in)
		session := ""
		query := ""
		if in != nil {
			session = in.SessionID
			query = in.Query
		}

		if judgeModel == nil || strings.TrimSpace(systemPrompt) == "" {
			st.VerdictKind = flow.VerdictJudge
			lg.Warn("judge missing, fail-closed", slog.String("session", session))
			return nil, flow.ErrSkipReply
		}

		safe, err := classifyByJudge(ctx, judgeModel, systemPrompt, query)
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

func classifyByJudge(ctx context.Context, judgeModel model.BaseChatModel, systemPrompt, input string) (bool, error) {
	messages := []*schema.Message{
		schema.SystemMessage(systemPrompt),
		schema.UserMessage("<input>\n" + input + "\n</input>"),
	}
	msg, err := judgeModel.Generate(ctx, messages)
	if err != nil {
		return false, err
	}

	content := strings.ToLower(strings.TrimSpace(msg.Content))
	content = strings.Trim(content, "\"'. \n\t")
	switch content {
	case judgeTokenSafe:
		return true, nil
	case judgeTokenAttack:
		return false, nil
	default:
		return false, nil
	}
}
