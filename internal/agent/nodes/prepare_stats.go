// Package nodes 的 prepare_stats.go 实现"对话前准备 stats 快照"的 Lambda 节点。
//
// 位置：regexGate(放行) → prepareStats → loadHistory。
//
// 本节点把懒结算 + 快照读取统一通过 stats.Store.Snapshot 完成；Snapshot 内部
// 一次 pipeline 读 mood / last_chat_at / affinity，必要时写回回归后的 mood。
// 它是 stats 进入 Graph 的唯一入口：下游 buildMessages 只消费 st.Affinity /
// st.Mood 平铺字段，不知道 Redis、结算策略或功能开关。stats 仍是装饰性功能：
// Store 方法自身软降级，节点也不返回错误。
package nodes

import (
	"context"
	"time"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/stats"
)

// NewPrepareStats 构造 prepareStats 节点。
//
// prepareStats 放在 buildMessages 之前，是为了让系统提示词拿到"本轮开始时"
// 的人设参数快照；放在 loadHistory 之前，则是因为 stats 与历史互不依赖，
// 尽早结算能把职责边界切清楚。
//
// store 为 nil 或 UserID 为空时直接跳过，st.Affinity / st.Mood 保持零值；
// Platform 即使为空也继续传给 stats.Store，由 Store 统一用 unknown 前缀兜底。
// 这样关闭 stats、私聊 / 群聊字段缺失、或上游平台尚未补齐 Platform 时，Graph
// 都能继续生成回复。
//
// Snapshot 内部会在读取的同时懒结算 mood 自然回归：长时间无人说话后，第一轮
// 回复也应当使用已经自然回归后的 mood，而不是把旧情绪再带进 prompt 一轮。
// 这个设计把"按时间自然冷却"挪到真实对话到来时懒结算，避免为了装饰性信号
// 常驻定时器。
func NewPrepareStats(store *stats.Store) *compose.Lambda {
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		if store == nil || st == nil || st.In == nil || st.In.UserID == "" {
			return st, nil
		}
		snap := store.Snapshot(ctx, st.In.Platform, st.In.UserID, time.Now())
		st.Affinity = snap.Affinity
		st.Mood = snap.Mood
		return st, nil
	})
}
