// Package nodes 的 chat_model.go 实现主聊天模型生成节点。
//
// 位置：buildMessages → chatModel → postproc。
// 输入已是 buildMessages 组装好的完整消息列表，本节点只调用主模型并写入 Reply。
package nodes

import (
	"context"
	"fmt"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
)

// NewChatModel 构造 chatModel 节点。
func NewChatModel(mainModel model.BaseChatModel) *compose.Lambda {
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		reply, err := mainModel.Generate(ctx, st.Messages)
		if err != nil {
			return nil, fmt.Errorf("agent: main chain: %w", err)
		}
		st.Reply = reply
		return st, nil
	})
}
