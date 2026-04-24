// Package nodes 的 build_messages.go 实现"把历史 + 用户 Query 组装成 LLM 消息列表"
// 的 Lambda 节点。
//
// 位置：loadHistory → buildMessages → guardedModel。
//
// 为什么独立成节点：
//   - 消息组装形态（是否加 system prompt、是否包裹 <user_input>、system prompt
//     的具体内容）属于"人设/模板"关切，与 guard / 主链调用完全正交；
//   - 把它独立出来，未来引入 RAG 检索、Few-shot 示例、工具调用指令时，只需在
//     本节点之后插入新节点修改 Messages，而不必改 guardedModel；
//   - 本节点不 import agent 包：为了避免循环依赖（agent.Build → nodes，若 nodes
//     又反向依赖 agent.Persona 就是环），构造器接收一个 build 函数字面量，
//     由 agent.Build 用 persona.BuildMessages 做闭包注入。这让 nodes 仅依赖 flow。
package nodes

import (
	"context"
	"fmt"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/stats"
)

// BuildFunc 是"历史 + 原始 Query + 人设参数快照 → 完整消息列表"的契约。
// 典型实现是 agent.Persona.BuildMessages；测试时可以用一个 stub 替换。
//
// snap 零值表示"无信号"（见 stats.Snapshot 文档）——BuildFunc 约定在零值
// 时不追加状态行，因此 loadStats 未注入时传 stats.Snapshot{} 即可，不需要
// 另外的开关参数。
type BuildFunc func(history []*schema.Message, query string, snap stats.Snapshot) ([]*schema.Message, error)

// LoadStatsFunc 返回某用户当前的人设参数快照。允许为 nil——表示 stats 功能
// 关闭，buildMessages 节点会直接用 stats.Snapshot{} 调用 BuildFunc。
//
// 签名刻意与 (*stats.Store).Snapshot 对齐（platform, userID），不返回 error：
// stats 读属于装饰性功能，任何失败都应 fail-soft 返回零值，而不是让本节点
// 把错误冒泡到 Graph 中断正常对话。
//
// platform 用来把 affinity hash 里的 field 按平台隔开，避免跨平台同号撞车。
type LoadStatsFunc func(ctx context.Context, platform, userID string) stats.Snapshot

// NewBuildMessages 构造 buildMessages 节点。
//
// build 必须非 nil——若为 nil 则在调用期才爆炸，反而不如启动期直接 panic 更"快失败"。
// loadStats 允许 nil：nil 表示"不做参数读取"，完全等价于 stats.enabled=false。
//
// 为什么把 Redis 读放在本节点内部，而不是新增一个独立的 loadStats 节点：
//  1. 参数快照的唯一消费者就是 BuildMessages 本身——用来给 system prompt 追加
//     调制行。没有第二个下游节点会再读它（guardedModel / postproc / saveHistory
//     都不关心这些参数）；
//  2. 独立节点意味着要改 Graph 拓扑、多一条边、多一个可能失败的阶段，
//     但换不回任何可复用性，纯粹增加复杂度；
//  3. 整个 stats 链路的关键性能考量在于"异步打分不阻塞主回复"，而读本身
//     就是阻塞路径的一部分——内联在这里最自然。
//
// 一旦将来真有第二个消费者（比如要把参数值也写进日志/遥测），再把本段
// 提取为独立节点并不难，届时再动拓扑也不迟。
func NewBuildMessages(build BuildFunc, loadStats LoadStatsFunc) *compose.Lambda {
	if build == nil {
		panic("agent/nodes: BuildFunc must not be nil")
	}
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		// loadStats 未注入或 UserID 缺失时 st.Stats 保持零值，BuildFunc 会
		// 跳过状态行。这条路径同时覆盖"stats 关闭"和"上游忘记填 UserID"
		// 两种场景——fail-soft，不拖累主对话。
		// Platform 即使为空，loadStats 里也会有兜底（用 "unknown" 前缀），
		// 因此不拿它做短路条件——漏传 platform 不该静默丢失好感度记录。
		if loadStats != nil && st.In.UserID != "" {
			st.Stats = loadStats(ctx, st.In.Platform, st.In.UserID)
		}
		msgs, err := build(st.History, st.In.Query, st.Stats)
		if err != nil {
			return nil, fmt.Errorf("agent: build messages: %w", err)
		}
		st.Messages = msgs
		return st, nil
	})
}
