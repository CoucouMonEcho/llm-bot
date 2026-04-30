// Package nodes 的 load_context.go 实现"读取本轮对话上下文"的复合节点。
//
// 位置：judgeGate(放行) → loadContext → lowStateGate。
// 内部顺序固定为 stats snapshot → memory load → history load → group background；
// 这些上下文都是 prompt 装饰信号，读取失败时降级为缺省值，不阻断主聊天链路。
package nodes

import (
	"cmp"
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/memory"
	"github.com/echo/llm-bot/internal/stats"
	"github.com/echo/llm-bot/internal/store"
)

const personalHistorySupplementSize = 4

// groupBackgroundMaxChars 是渲染后的群聊背景块允许占用的总字符（rune）上限。
// 写入侧已用 LTRIM/EXPIRE 管控规模，这里只做"防极端长消息撑爆 prompt"的兜底；
// 不外化成配置项，避免为一个内部安全阀增加配置面。
const groupBackgroundMaxChars = 1200

// NewLoadContext 构造 loadContext 节点。
//
// groupBuffer 可为 nil，表示关闭群聊短期上下文背景注入；节点内部按 nil 跳过。
func NewLoadContext(
	statsStore *stats.Store,
	memoryStore *memory.Store,
	historyRepo store.HistoryRepo,
	groupBuffer store.GroupBufferRepo,
	memoryMaxChars, historySize int,
	logger *slog.Logger,
) *compose.Lambda {
	lg := cmp.Or(logger, slog.Default()).With(slog.String("node", "loadContext"))
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		loadStatsSnapshot(ctx, st, statsStore)
		loadUserMemory(ctx, st, memoryStore, memoryMaxChars, lg)
		loadSessionHistory(ctx, st, historyRepo, historySize, lg)
		loadGroupBackground(ctx, st, groupBuffer, lg)
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

// loadGroupBackground 把短期群聊缓存渲染成一段"刚才群里在聊什么"的背景文本。
//
// 仅在群聊会话调用：私聊场景没有"群里"的概念，硬塞背景反而污染人设。读取失败
// 时降级为空串而不返回 error，与 stats/memory/history 同步的"装饰信号"语义保持一致。
// 不再在本侧做时间窗 / 数量过滤——写入侧的 LTRIM/EXPIRE 已经管控规模，本侧重复
// 实现只会让两处口径漂移。
func loadGroupBackground(ctx context.Context, st *flow.State, repo store.GroupBufferRepo, lg *slog.Logger) {
	if repo == nil || st == nil || st.In == nil || st.In.ConvType != "group" || st.In.SessionID == "" {
		return
	}
	entries, err := repo.Load(ctx, st.In.SessionID)
	if err != nil {
		lg.Warn("load group buffer failed, fallback to empty",
			slog.String("session", st.In.SessionID),
			slog.Any("err", err))
		return
	}
	if len(entries) == 0 {
		return
	}
	st.GroupBackground = renderGroupBackground(entries, groupBackgroundMaxChars)
}

// renderGroupBackground 把缓存条目按行拼接成"[时间] <名称> 说: <内容>"格式；
// 时间缺失时省略前缀（与 history.go 同源容错），名称按 UserName→UserID→"路人" 三级回退。
//
// 超过 maxRunes 时从尾部往前累加保留最近若干行——背景块的价值集中在"最近的氛围"，
// 截掉前面的旧消息比截掉后面的新消息更合理。maxRunes <= 0 表示不限。
func renderGroupBackground(entries []store.GroupBufferEntry, maxRunes int) string {
	lines := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.UserName
		if name == "" {
			name = e.UserID
		}
		if name == "" {
			name = "路人"
		}
		var b strings.Builder
		if !e.Time.IsZero() {
			b.WriteByte('[')
			b.WriteString(e.Time.Local().Format("2006-01-02 15:04"))
			b.WriteString("] ")
		}
		b.WriteString(name)
		b.WriteString(" 说: ")
		b.WriteString(e.Content)
		lines = append(lines, b.String())
	}

	if maxRunes <= 0 {
		return strings.Join(lines, "\n")
	}

	// 从尾部往前累加 rune 长度，找到第一个会让总长度超限的边界。
	// 即便单行就已超限也至少保留这一行——空背景块没有信息价值。
	total := 0
	start := len(lines)
	for i := len(lines) - 1; i >= 0; i-- {
		extra := len([]rune(lines[i]))
		if i < len(lines)-1 {
			extra++ // 行间换行
		}
		if total+extra > maxRunes && i < len(lines)-1 {
			break
		}
		total += extra
		start = i
	}
	return strings.Join(lines[start:], "\n")
}
