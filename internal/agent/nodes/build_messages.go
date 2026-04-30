// Package nodes 的 build_messages.go 实现"把长期记忆 + 历史 + 用户 Query 组装成
// LLM 消息列表"的 Lambda 节点。
//
// 位置：lowStateGate → buildMessages → chatModel。
//
// 为什么独立成节点：
//   - 消息组装形态（是否加系统提示词、如何设置消息 name、系统提示词的具体
//     内容）属于"人设/模板"关切，与 guard / 主链调用完全正交；
//   - 把它独立出来，未来引入 RAG 检索、Few-shot 示例、工具调用指令时，只需在
//     本节点之后插入新节点修改 Messages，而不必改 chatModel；
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
)

// BuildFunc 是"历史 + 原始 Query + 用户 ID + 人设参数（好感度 / 心情）+
// 长期记忆 + 群聊背景 → 完整消息列表"的契约。
// 典型实现是 agent.Persona.BuildMessages；测试时可以用一个替身函数替换。
//
// affinity / mood 同时为 0 表示"无信号"（stats 关闭、读 Redis 失败或 UserID
// 缺失）——此时 PromptLine 只渲染当前时间行而不暴露关系/心情标签；调用方
// 传 0,0 即可，不需要额外的开关参数。
//
// groupBackground 仅在群聊会话且 GroupBuffer 仓库启用时由 loadContext 写入；
// 私聊、缓存关闭、读取失败均传空串，由实现方负责"空时不注入背景块"。
type BuildFunc func(history []*schema.Message, query, userID string, affinity, mood int, memory string, groupBackground string) ([]*schema.Message, error)

// NewBuildMessages 构造 buildMessages 节点。
//
// build 必须非 nil——若为 nil 则在调用期才爆炸，反而不如启动期直接 panic 更"快失败"。
// stats 平铺字段、长期记忆与群聊背景都由上游节点提前写入 State；本节点只负责
// prompt 组装，不访问外部存储。
func NewBuildMessages(build BuildFunc) *compose.Lambda {
	if build == nil {
		panic("agent/nodes: BuildFunc must not be nil")
	}
	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		msgs, err := build(st.History, st.In.Query, st.In.UserID, st.Affinity, st.Mood, st.Memory, st.GroupBackground)
		if err != nil {
			return nil, fmt.Errorf("agent: build messages: %w", err)
		}
		st.Messages = msgs
		return st, nil
	})
}
