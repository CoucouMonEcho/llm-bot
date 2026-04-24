// Package nodes 的 load_history.go 实现"从 Redis 读取会话历史"的 Lambda 节点。
//
// 位置：regexGate(miss) → loadHistory → buildMessages → guardedModel。
//
// 为什么独立成节点：
//   - 历史读取是纯 IO，与消息组装、模型调用解耦，便于未来替换为别的存储
//     （内存 Map / PostgreSQL / 云端会话服务）而无需动其他节点；
//   - 读取失败不应阻断对话——本节点吞错打 warn，把 History 置空切片，
//     把"降级为无历史对话"的策略集中在一处，避免每个下游都写一遍同样的兜底。
package nodes

import (
	"context"
	"log/slog"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/store"
)

// NewLoadHistory 构造 loadHistory 节点。
//
// historySize 控制一次读多少条；<=0 时 Redis 侧会按"不裁剪"处理，
// 不推荐长期使用。
func NewLoadHistory(repo store.HistoryRepo, historySize int, logger *slog.Logger) *compose.Lambda {
	lg := logger.With(slog.String("node", "loadHistory"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		history, err := repo.Load(ctx, st.In.SessionID, historySize)
		if err != nil {
			lg.Warn("load history failed, fallback to empty",
				slog.String("session", st.In.SessionID),
				slog.Any("err", err))
			st.History = nil
			return st, nil
		}
		st.History = history
		return st, nil
	})
}
