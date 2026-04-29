// Package nodes 的 load_memory.go 实现"从 Redis 读取长期用户记忆"的 Lambda 节点。
//
// 位置：prepareStats → loadMemory → loadHistory。
//
// 本节点只把按人头压缩后的事实摘要写入 st.Memory；它不参与短期 history 的
// schema.Message 序列，避免把"长期事实"伪装成某一轮真实对话。
package nodes

import (
	"context"
	"log/slog"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/memory"
)

// NewLoadMemory 构造 loadMemory 节点。
//
// store 为 nil 或 UserID 为空时直接跳过，st.Memory 保持空字符串。长期记忆是
// 提升真实感的装饰性上下文，不应让 Redis 故障影响正常聊天。
func NewLoadMemory(store *memory.Store, maxChars int, logger *slog.Logger) *compose.Lambda {
	lg := logger.With(slog.String("node", "loadMemory"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		if store == nil || st == nil || st.In == nil || st.In.UserID == "" {
			return st, nil
		}
		st.Memory = store.Load(ctx, st.In.Platform, st.In.UserID, maxChars)
		if st.Memory != "" {
			lg.Debug("memory loaded", slog.String("session", st.In.SessionID))
		}
		return st, nil
	})
}
