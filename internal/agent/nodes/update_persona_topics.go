// Package nodes 的 update_persona_topics.go 实现"回复生成后触发闲聊话题锚点更新"的 Lambda 节点。
//
// 位置：saveHistory → updateMemory → updatePersonaTopics → scoreStats。
//
// 本节点只触发异步 Dispatch，不等待模型整理或 Redis 写入。话题锚点只服务于之后
// 的自然承接，不应拖慢当前回复收尾。
package nodes

import (
	"context"
	"log/slog"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/persona"
)

// NewUpdatePersonaTopics 构造 updatePersonaTopics 节点。
//
// 只有正常回复路径会经过 saveHistory / updateMemory 再来到这里；被 guard 拦截的
// 攻击消息不会进入话题锚点，避免污染之后闲聊。
func NewUpdatePersonaTopics(store *persona.Store, updateModel model.BaseChatModel, updatePrompt string, maxItems int, maxAge time.Duration, logger *slog.Logger) *compose.Lambda {
	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		if store == nil || updateModel == nil {
			return st, nil
		}
		if st == nil || st.In == nil || st.In.Query == "" {
			return st, nil
		}
		if st.Reply == nil || st.Reply.Content == "" {
			return st, nil
		}
		persona.Dispatch(store, updateModel, updatePrompt, logger,
			st.In.Query, st.Reply.Content, maxItems, maxAge)
		return st, nil
	})
}
