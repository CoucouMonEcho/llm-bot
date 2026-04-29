// Package nodes 的 update_memory.go 实现"回复生成后触发长期记忆更新"的 Lambda 节点。
//
// 位置：saveHistory → updateMemory → scoreStats。
//
// 与 scoreStats 一样，本节点只触发异步 Dispatch，不等待模型总结或 Redis 写入。
// 长期记忆服务于之后的对话真实感，不能拖慢当前回复收尾。
package nodes

import (
	"context"
	"log/slog"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/memory"
)

// NewUpdateMemory 构造 updateMemory 节点。
//
// 只有正常回复路径会经过 saveHistory 再来到这里；被 guard 拦截的攻击消息不会写入
// 长期记忆，避免恶意输入污染用户画像。
func NewUpdateMemory(store *memory.Store, updateModel model.BaseChatModel, updatePrompt string, maxChars int, logger *slog.Logger) *compose.Lambda {
	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		if store == nil || updateModel == nil {
			return st, nil
		}
		if st == nil || st.In == nil || st.In.UserID == "" || st.In.Query == "" {
			return st, nil
		}
		if st.Reply == nil || st.Reply.Content == "" {
			return st, nil
		}
		memory.Dispatch(store, updateModel, updatePrompt, logger,
			st.In.Platform, st.In.UserID, st.Memory, st.In.Query, st.Reply.Content, maxChars)
		return st, nil
	})
}
