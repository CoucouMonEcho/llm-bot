// Package nodes 的 score_stats.go 实现"回复生成后触发 stats 打分"的 Lambda 节点。
//
// 位置：
//   - 正常路径：saveHistory → scoreStats → END
//   - 降级路径：fallback → scoreStats → END
//
// 本节点只触发异步 Dispatch，不等待打分模型与 Redis 写入完成。触发点是
// "Agent 已经生成回复"，而不是 Adapter 发送成功：stats 描述的是这轮对话
// 对人设参数的影响，不应被平台发送成功与否绑住。降级路径也会到这里，
// 因为攻击/调戏本身同样会影响好感度与心情。
package nodes

import (
	"context"
	"log/slog"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/stats"
)

// NewScoreStats 构造 scoreStats 节点。
//
// store / scoreModel 任一为空、缺少用户或输入、回复为空时都直接跳过。
// 这些条件都代表"没有足够信息形成一轮可打分对话"，跳过比返回错误更符合
// stats 的装饰性定位。
//
// 这里不复用调用方 ctx：Dispatch 内部会创建独立超时上下文，避免 Bot 在发送
// 完回复后取消请求 ctx，顺手把异步打分也取消掉。反过来，打分慢或失败也不能
// 拖住 Graph 收尾，所以本节点只做触发，不把结果写回 State。
func NewScoreStats(store *stats.Store, scoreModel model.BaseChatModel, scorePrompt string, logger *slog.Logger) *compose.Lambda {
	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		if store == nil || scoreModel == nil {
			return st, nil
		}
		if st == nil || st.In == nil || st.In.UserID == "" || st.In.Query == "" {
			return st, nil
		}
		if st.Reply == nil || st.Reply.Content == "" {
			return st, nil
		}
		stats.Dispatch(store, scoreModel, scorePrompt, logger,
			st.In.Platform, st.In.UserID, st.In.Query, st.Reply.Content)
		return st, nil
	})
}
