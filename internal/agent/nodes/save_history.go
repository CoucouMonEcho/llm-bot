// Package nodes 的 save_history.go 实现"把本轮对话写回 Redis"的 Lambda 节点。
//
// 位置：postproc → saveHistory → END。
// 只有 "正常回复" 路径会走到这里——被 guard 判定为攻击的消息及其降级回复
// 都通过 branch 直接跳到 fallback → END，从而天然地**不入历史**。
//
// 写入内容：
//   - 一条 user 消息（st.In.Query 原文，不做 wrapper 包装——wrapper 只是
//     喂给 LLM 的形式，不应污染将来的上下文）；
//   - 一条 assistant 消息（st.Reply.Content，清洗后的版本）。
//
// 写入顺序必须 user 先 assistant 后——history List 的语义约束。
package nodes

import (
	"context"
	"log/slog"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/store"
)

// NewSaveHistory 构造 saveHistory 节点。
//
// historyMax 用于节点内部 LTRIM，控制单会话历史长度；
// 若 <=0 则不裁剪（不推荐）。
func NewSaveHistory(repo store.HistoryRepo, historyMax int, logger *slog.Logger) *compose.Lambda {
	lg := logger.With(slog.String("node", "saveHistory"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		if st.Reply == nil || st.Reply.Content == "" {
			return st, nil
		}

		// Step 1: 写入用户消息（使用原始 Query，不是 wrapper 后的）。
		userMsg := schema.UserMessage(st.In.Query)
		if err := repo.Append(ctx, st.In.SessionID, userMsg, historyMax); err != nil {
			lg.Warn("append user message failed",
				slog.String("session", st.In.SessionID),
				slog.Any("err", err))
			return st, nil // 不阻断回复发送
		}

		// Step 2: 写入 assistant 回复。
		if err := repo.Append(ctx, st.In.SessionID, st.Reply, historyMax); err != nil {
			lg.Warn("append assistant message failed",
				slog.String("session", st.In.SessionID),
				slog.Any("err", err))
		}
		return st, nil
	})
}
