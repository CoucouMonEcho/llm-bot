// Package nodes 的 load_context.go 实现"读取本轮对话上下文"的复合节点。
//
// 位置：judgeGate(放行) → loadContext → buildMessages。
// 内部顺序固定为 stats snapshot → memory load → history load；这些上下文都是
// prompt 装饰信号，读取失败时降级为缺省值，不阻断主聊天链路。
package nodes

import (
	"cmp"
	"context"
	"log/slog"
	"time"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/memory"
	"github.com/echo/llm-bot/internal/stats"
	"github.com/echo/llm-bot/internal/store"
)

const personalHistorySupplementSize = 4

// NewLoadContext 构造 loadContext 节点。
func NewLoadContext(statsStore *stats.Store, memoryStore *memory.Store, historyRepo store.HistoryRepo, memoryMaxChars, historySize int, logger *slog.Logger) *compose.Lambda {
	lg := cmp.Or(logger, slog.Default()).With(slog.String("node", "loadContext"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		loadStatsSnapshot(ctx, st, statsStore)
		loadUserMemory(ctx, st, memoryStore, memoryMaxChars, lg)
		loadSessionHistory(ctx, st, historyRepo, historySize, lg)
		return st, nil
	})
}

func loadStatsSnapshot(ctx context.Context, st *flow.State, store *stats.Store) {
	if store == nil || st == nil || st.In == nil || st.In.UserID == "" {
		return
	}
	snap := store.Snapshot(ctx, st.In.Platform, st.In.UserID, time.Now())
	st.Affinity = snap.Affinity
	st.Mood = snap.Mood
}

func loadUserMemory(ctx context.Context, st *flow.State, store *memory.Store, maxChars int, lg *slog.Logger) {
	if store == nil || st == nil || st.In == nil || st.In.UserID == "" {
		return
	}
	st.Memory = store.Load(ctx, st.In.Platform, st.In.UserID, maxChars)
	if st.Memory != "" {
		lg.Debug("memory loaded", slog.String("session", st.In.SessionID))
	}
}

func loadSessionHistory(ctx context.Context, st *flow.State, repo store.HistoryRepo, historySize int, lg *slog.Logger) {
	if repo == nil || st == nil || st.In == nil {
		return
	}
	history, err := repo.Load(ctx, st.In.SessionID, historySize)
	if err != nil {
		lg.Warn("load history failed, fallback to empty",
			slog.String("session", st.In.SessionID),
			slog.Any("err", err))
		st.History = nil
		return
	}
	if st.In.ConvType == "group" && st.In.UserID != "" {
		personalSessionID := "private_" + st.In.UserID
		personal, err := repo.Load(ctx, personalSessionID, personalHistorySupplementSize)
		if err != nil {
			// 个人历史只是群聊里的补充上下文，失败时保留群聊主线即可。
			lg.Warn("load personal supplement failed, keep session history",
				slog.String("session", st.In.SessionID),
				slog.String("personalSession", personalSessionID),
				slog.Any("err", err))
		} else {
			history = mergePersonalSupplement(personal, history)
		}
	}
	st.History = history
}

func mergePersonalSupplement(personal, session []*schema.Message) []*schema.Message {
	if len(personal) == 0 {
		return session
	}
	seen := make(map[string]struct{}, len(personal)+len(session))
	for _, msg := range session {
		if msg == nil {
			continue
		}
		seen[historyMessageKey(msg)] = struct{}{}
	}

	merged := make([]*schema.Message, 0, len(personal)+len(session))
	for _, msg := range personal {
		if msg == nil {
			continue
		}
		key := historyMessageKey(msg)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		merged = append(merged, msg)
	}
	merged = append(merged, session...)
	return merged
}

func historyMessageKey(msg *schema.Message) string {
	return string(msg.Role) + "\x00" + msg.Name + "\x00" + msg.Content
}
