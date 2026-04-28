// Package guard 的 regex_gate.go 实现"第一级防线"的独立 Lambda 节点。
//
// 位置：START → regexGate → (branch by Verdict)
//   - 命中任一黑名单 → st.Verdict = {Kind: VerdictRegex, Detail: pattern}，
//     随后 branch 路由到 fallback；
//   - 未命中 → Verdict 保持零值（Safe），继续流向 prepareStats。
//
// 为什么独立成节点：
//   - 正则检测是 Graph 的唯一"纯同步、无 IO、无 LLM"环节，独立出来可以让
//     整个防御链在 Graph 拓扑上一眼看清"先低成本过滤再高成本判定"；
//   - 本节点也是唯一接受 *flow.Input 的节点（START 的输出），由它负责把
//     Input 装箱成 State，下游就都以 *flow.State 统一流转。
//
// 本节点始终返回 nil error——检测结果通过 Verdict 字段外化，由 Graph branch
// 决策走向；这样 Graph 的"错误路径"就能只被真正的系统故障占用，语义更干净。
package guard

import (
	"context"
	"log/slog"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
)

// NewRegexGate 构造 regexGate 节点。
//
// regex 为 nil 表示"关闭正则层"，此时节点仅完成 Input→State 的装箱。
func NewRegexGate(regex *RegexMatcher, logger *slog.Logger) *compose.Lambda {
	lg := logger.With(slog.String("node", "regexGate"))
	return compose.InvokableLambda(func(_ context.Context, in *flow.Input) (*flow.State, error) {
		st := flow.NewState(in)
		if regex == nil {
			return st, nil
		}
		pattern, matched := regex.Match(in.Query)
		if !matched {
			return st, nil
		}
		st.Verdict = flow.Verdict{Kind: flow.VerdictRegex, Detail: pattern}
		lg.Info("regex blocked",
			slog.String("session", in.SessionID),
			slog.String("pattern", pattern))
		return st, nil
	})
}
