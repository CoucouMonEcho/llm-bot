// Package nodes 的 save_history.go 实现"把本轮对话写回 Redis"的 Lambda 节点。
//
// 位置：postproc → saveHistory → updateMemory → scoreStats → END。
// 只有"正常回复"路径会走到这里——被 guard 判定为攻击的消息会静默中断，
// 不发回复、不入历史，也不触发回复后副作用。
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
	"github.com/echo/llm-bot/internal/infra/store"
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

		targets := []string{st.In.SessionID}
		if st.In.ConvType == "group" && st.In.UserID != "" {
			targets = append(targets, "private_"+st.In.UserID)
		}

		for _, sessionID := range targets {
			appendTurn(ctx, repo, historyMax, lg, sessionID, st.In.Query, st.In.UserID, st.Reply.Content)
		}
		return st, nil
	})
}

func appendTurn(ctx context.Context, repo store.HistoryRepo, historyMax int, lg *slog.Logger, sessionID, query, userID, reply string) {
	// 第一步：写入用户消息（使用原始 Query，不是 wrapper 后的）。
	userMsg := &schema.Message{
		Role:    schema.User,
		Content: query,
		Name:    userID,
	}
	if err := repo.Append(ctx, sessionID, userMsg, historyMax); err != nil {
		lg.Warn("append user message failed",
			slog.String("session", sessionID),
			slog.Any("err", err))
		return // 不写半个 turn，但不阻断回复发送
	}

	// 第二步：写入 assistant 回复。
	assistantMsg := schema.AssistantMessage(reply, nil)
	if err := repo.Append(ctx, sessionID, assistantMsg, historyMax); err != nil {
		lg.Warn("append assistant message failed",
			slog.String("session", sessionID),
			slog.Any("err", err))
	}
}
