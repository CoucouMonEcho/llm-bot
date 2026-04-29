// Package proactive 维护主动消息的候选选择、生成与发送状态。
//
// 本文件集中定义 Redis key、状态读写和轻量序列化结构。本包不依赖
// bot/agent 编排层；外层决定何时记录入站消息、何时调度发送。
package proactive

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/echo/llm-bot/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	// Redis key 约定：
	//   - 运行期开关必须和配置开关同时打开才会真正发送；
	//   - 群白名单控制哪些群允许被主动唤起；
	//   - 用户/会话索引用于从好感度用户回找可触达会话；
	//   - 近期群活动只记录白名单群的入站消息，供兜底策略挑选；
	//   - 最近主动发送时间和日计数负责冷却、日限额；
	//   - PendingContext 保存刚主动发出的短期上下文，供下一条用户回复承接。
	keyEnabled           = "bot_proactive_enabled"
	keyGroupWhitelist    = "bot_proactive_group_whitelist"
	keyUserLastInbound   = "bot_proactive_user_last_inbound"
	keyRecentGroupEvents = "bot_proactive_whitelist_group_events"
	keyLastProactiveAt   = "bot_proactive_last_proactive_at"

	keyUserSessionsPrefix = "bot_proactive_user_sessions_"
	keySessionMetaPrefix  = "bot_proactive_session_meta_"
	keyPendingPrefix      = "bot_proactive_pending_"
	keyDailyCountPrefix   = "bot_proactive_daily_count_"

	// stats 模块维护的好感度排行是主动选择的事实来源；proactive 只读不写，
	// 避免两个包同时拥有好感度语义。
	keyAffinityRank = "bot_stats_affinity_rank"

	// 近期群活动只保留最近一小段，避免 Redis List 随群聊流量无限增长。
	defaultRecentGroupEventCap = 200
	// SessionMeta 是可触达性的近似缓存；过期后由新的入站消息自然重建。
	sessionMetaTTL = 30 * 24 * time.Hour
	// 日计数横跨本地日期边界保留两天，给跨零点调度留出读取余量。
	dailyCountTTL = 48 * time.Hour
	// PendingContext 只服务“刚被主动喊完后的下一轮承接”，不应长期污染上下文。
	defaultPendingTTL = 30 * time.Minute
)

// State 封装主动消息用到的 Redis key 与命令形态。
//
// 这里不做内存缓存，Redis 是主动状态的唯一真相源；多进程部署时只要外层保证
// 调度器单实例，记录器可以在多个 bot 实例上安全写入。
type State struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewState 构造主动消息状态层。
func NewState(rdb *redis.Client, log *slog.Logger) *State {
	return &State{rdb: rdb, log: cmp.Or(log, slog.Default())}
}

// Enabled 读取运行期开关；缺失或格式异常都按关闭处理。
//
// 运行期开关故意和 YAML 配置开关分离：配置决定功能是否接线，Redis 开关让运维
// 可以在不重启进程的情况下临时启停主动消息。
func (s *State) Enabled(ctx context.Context) (bool, error) {
	raw, err := s.rdb.Get(ctx, keyEnabled).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("proactive: read enabled: %w", err)
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		s.log.Warn("proactive enabled switch parse failed", "value", raw, "err", err)
		return false, nil
	}
	return enabled, nil
}

// SetEnabled 更新运行期开关。
//
// key 不设置 TTL，避免 Redis 里开关意外过期后从“已开启”悄悄退回关闭状态。
func (s *State) SetEnabled(ctx context.Context, enabled bool) error {
	if err := s.rdb.Set(ctx, keyEnabled, strconv.FormatBool(enabled), 0).Err(); err != nil {
		return fmt.Errorf("proactive: set enabled: %w", err)
	}
	return nil
}

// SetGroupWhitelisted 把群会话加入或移出运行期白名单。
//
// 只允许白名单群被主动唤起；私聊不走这层限制，而是依赖用户最近活动和会话冷却。
func (s *State) SetGroupWhitelisted(ctx context.Context, groupSessionID string, allowed bool) error {
	groupSessionID = normalizeGroupSessionID(groupSessionID)
	if groupSessionID == "" {
		return fmt.Errorf("proactive: empty group session id")
	}
	if allowed {
		if err := s.rdb.SAdd(ctx, keyGroupWhitelist, groupSessionID).Err(); err != nil {
			return fmt.Errorf("proactive: whitelist group: %w", err)
		}
		return nil
	}
	if err := s.rdb.SRem(ctx, keyGroupWhitelist, groupSessionID).Err(); err != nil {
		return fmt.Errorf("proactive: unwhitelist group: %w", err)
	}
	return nil
}

// GroupWhitelisted 判断群会话是否在运行期白名单里。
func (s *State) GroupWhitelisted(ctx context.Context, groupSessionID string) (bool, error) {
	groupSessionID = normalizeGroupSessionID(groupSessionID)
	if groupSessionID == "" {
		return false, nil
	}
	ok, err := s.rdb.SIsMember(ctx, keyGroupWhitelist, groupSessionID).Result()
	if err != nil {
		return false, fmt.Errorf("proactive: check group whitelist: %w", err)
	}
	return ok, nil
}

// WhitelistedGroups 返回当前允许主动唤起的群会话 ID。
//
// 返回值主要供管理命令展示，不参与选择流程里的强一致判断。
func (s *State) WhitelistedGroups(ctx context.Context) ([]string, error) {
	groups, err := s.rdb.SMembers(ctx, keyGroupWhitelist).Result()
	if err != nil {
		return nil, fmt.Errorf("proactive: list group whitelist: %w", err)
	}
	return groups, nil
}

// RecordUserLastInbound 记录用户最近一次主动发言时间。
//
// 使用 ZSET 是为了按时间窗口筛选时保留扩展空间；当前选择器按 member 点查，
// 将来可以直接按 score 范围扫描沉寂用户。
func (s *State) RecordUserLastInbound(ctx context.Context, platform domain.Platform, userID string, at time.Time) error {
	if userID == "" {
		return fmt.Errorf("proactive: empty user id")
	}
	if err := s.rdb.ZAdd(ctx, keyUserLastInbound, redis.Z{
		Score:  float64(at.Unix()),
		Member: makeUserKey(platform, userID),
	}).Err(); err != nil {
		return fmt.Errorf("proactive: record user last inbound: %w", err)
	}
	return nil
}

// UserLastInbound 读取用户最近一次主动发言时间。
//
// bool 返回值区分“没有记录”和“记录值为 Unix 零点”，避免调用方把冷启动误判为
// 一个极旧的真实活跃时间。
func (s *State) UserLastInbound(ctx context.Context, platform domain.Platform, userID string) (time.Time, bool, error) {
	score, err := s.rdb.ZScore(ctx, keyUserLastInbound, makeUserKey(platform, userID)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("proactive: read user last inbound: %w", err)
	}
	return time.Unix(int64(score), 0), true, nil
}

// SessionMeta 是重新触达一个已知会话所需的最小信息。
//
// 它由入站消息持续刷新，只保存发送所需字段和最近见到的用户昵称；不存完整消息，
// 避免把长期历史复制到 proactive 状态里。
type SessionMeta struct {
	Platform     domain.Platform         `json:"platform"`
	ConvType     domain.ConversationType `json:"conv_type"`
	SessionID    string                  `json:"session_id"`
	LastUserID   string                  `json:"last_user_id,omitempty"`
	LastUserName string                  `json:"last_user_name,omitempty"`
	LastSeenUnix int64                   `json:"last_seen_unix"`
}

// ref 返回跨平台唯一的会话引用，供用户会话索引和冷却 map 复用。
func (m SessionMeta) ref() string {
	return makeSessionRef(m.Platform, m.SessionID)
}

// RecordSession 建立“用户 -> 会话”的反查索引，并刷新会话元数据。
//
// user_sessions 与 session_meta 使用相同 TTL：用户长期不再出现时，可触达索引
// 会自然消失，避免主动消息打到过旧会话。
func (s *State) RecordSession(ctx context.Context, msg *domain.InboundMessage, at time.Time) error {
	if msg == nil || msg.UserID == "" || msg.SessionID == "" || msg.Platform == "" || msg.ConvType == "" {
		return fmt.Errorf("proactive: incomplete session message")
	}

	ref := makeSessionRef(msg.Platform, msg.SessionID)
	userSessionsKey := keyUserSessionsPrefix + makeUserKey(msg.Platform, msg.UserID)
	metaKey := keySessionMetaPrefix + ref
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, userSessionsKey, ref)
	pipe.Expire(ctx, userSessionsKey, sessionMetaTTL)
	pipe.HSet(ctx, metaKey,
		"platform", string(msg.Platform),
		"conv_type", string(msg.ConvType),
		"session_id", msg.SessionID,
		"last_user_id", msg.UserID,
		"last_user_name", msg.UserName,
		"last_seen_unix", at.Unix(),
	)
	pipe.Expire(ctx, metaKey, sessionMetaTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("proactive: record session: %w", err)
	}
	return nil
}

// UserSessions 返回用户当前仍有元数据的已知会话；过期残留的引用会被忽略。
//
// 单个 meta 读取失败只记警告日志并跳过，保留“能触达谁就先用谁”的软降级行为。
func (s *State) UserSessions(ctx context.Context, platform domain.Platform, userID string) ([]SessionMeta, error) {
	refs, err := s.rdb.SMembers(ctx, keyUserSessionsPrefix+makeUserKey(platform, userID)).Result()
	if err != nil {
		return nil, fmt.Errorf("proactive: list user sessions: %w", err)
	}
	out := make([]SessionMeta, 0, len(refs))
	for _, ref := range refs {
		meta, ok, err := s.SessionMeta(ctx, ref)
		if err != nil {
			s.log.Warn("proactive session metadata read failed", "ref", ref, "err", err)
			continue
		}
		if ok {
			out = append(out, meta)
		}
	}
	return out, nil
}

// SessionMeta 按 session ref 读取一份会话元数据。
//
// 缺字段或解析失败时返回 ok=false，而不是错误；过期/半写入数据只代表该会话
// 暂时不可用，不应该中断整轮候选选择。
func (s *State) SessionMeta(ctx context.Context, ref string) (SessionMeta, bool, error) {
	vals, err := s.rdb.HGetAll(ctx, keySessionMetaPrefix+ref).Result()
	if err != nil {
		return SessionMeta{}, false, fmt.Errorf("proactive: read session meta: %w", err)
	}
	if len(vals) == 0 {
		return SessionMeta{}, false, nil
	}
	lastSeenUnix, _ := strconv.ParseInt(vals["last_seen_unix"], 10, 64)
	meta := SessionMeta{
		Platform:     domain.Platform(vals["platform"]),
		ConvType:     domain.ConversationType(vals["conv_type"]),
		SessionID:    vals["session_id"],
		LastUserID:   vals["last_user_id"],
		LastUserName: vals["last_user_name"],
		LastSeenUnix: lastSeenUnix,
	}
	if meta.Platform == "" || meta.ConvType == "" || meta.SessionID == "" {
		return SessionMeta{}, false, nil
	}
	return meta, true, nil
}

// RecentGroupEvent 是白名单群里一条最近入站消息的紧凑记录。
//
// 它服务“近期群活动”兜底策略，只保存生成开场白需要的短文本和身份线索。
type RecentGroupEvent struct {
	Platform  domain.Platform `json:"platform"`
	SessionID string          `json:"session_id"`
	UserID    string          `json:"user_id,omitempty"`
	UserName  string          `json:"user_name,omitempty"`
	Text      string          `json:"text,omitempty"`
	AtUnix    int64           `json:"at_unix"`
}

// at 把事件时间还原为 time.Time；缺失时间在生成提示词时会被省略。
func (e RecentGroupEvent) at() time.Time {
	if e.AtUnix <= 0 {
		return time.Time{}
	}
	return time.Unix(e.AtUnix, 0)
}

// AddRecentGroupEvent 写入白名单群的最近活动，并限制列表长度。
//
// LPUSH + LTRIM 保持新到旧顺序，也把群聊高频流量限制在固定 Redis 空间内。
func (s *State) AddRecentGroupEvent(ctx context.Context, event RecentGroupEvent, capLen int) error {
	if event.Platform == "" || event.SessionID == "" {
		return fmt.Errorf("proactive: incomplete group event")
	}
	if capLen <= 0 {
		capLen = defaultRecentGroupEventCap
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("proactive: marshal group event: %w", err)
	}
	pipe := s.rdb.Pipeline()
	pipe.LPush(ctx, keyRecentGroupEvents, data)
	pipe.LTrim(ctx, keyRecentGroupEvents, 0, int64(capLen-1))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("proactive: add group event: %w", err)
	}
	return nil
}

// RecentGroupEvents 读取最近白名单群活动，结果按新到旧排列。
//
// 单条 JSON 损坏只跳过并告警，不让一条坏记录阻断后续候选选择。
func (s *State) RecentGroupEvents(ctx context.Context, limit int) ([]RecentGroupEvent, error) {
	if limit <= 0 {
		limit = defaultRecentGroupEventCap
	}
	raws, err := s.rdb.LRange(ctx, keyRecentGroupEvents, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("proactive: read group events: %w", err)
	}
	events := make([]RecentGroupEvent, 0, len(raws))
	for _, raw := range raws {
		var event RecentGroupEvent
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			s.log.Warn("proactive group event parse failed", "err", err)
			continue
		}
		if event.Platform != "" && event.SessionID != "" {
			events = append(events, event)
		}
	}
	return events, nil
}

// AffinityEntry 是 stats 好感度排行 ZSET 中的一行。
//
// Score 保留 float64 是为了贴近 Redis ZSET 原始结果；选择策略只关心正负和排序。
type AffinityEntry struct {
	Platform domain.Platform
	UserID   string
	Score    float64
}

// TopAffinity 从 stats 排行中读取好感度最高的一批用户。
//
// member 解析失败会被跳过并告警，兼容 stats key 里可能存在的历史脏数据。
func (s *State) TopAffinity(ctx context.Context, n int) ([]AffinityEntry, error) {
	if n <= 0 {
		return nil, nil
	}
	rows, err := s.rdb.ZRevRangeWithScores(ctx, keyAffinityRank, 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("proactive: read affinity rank: %w", err)
	}
	out := make([]AffinityEntry, 0, len(rows))
	for _, row := range rows {
		member := fmt.Sprint(row.Member)
		platform, userID, ok := splitUserKey(member)
		if !ok || userID == "" {
			s.log.Warn("proactive affinity member skipped", "member", member)
			continue
		}
		out = append(out, AffinityEntry{
			Platform: platform,
			UserID:   userID,
			Score:    row.Score,
		})
	}
	return out, nil
}

// LastProactiveAt 读取会话最近一次主动发送时间，用于冷却判断。
//
// 未命中返回 ok=false，由选择器按“没有冷却记录”处理。
func (s *State) LastProactiveAt(ctx context.Context, platform domain.Platform, sessionID string) (time.Time, bool, error) {
	raw, err := s.rdb.HGet(ctx, keyLastProactiveAt, makeSessionRef(platform, sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("proactive: read last proactive at: %w", err)
	}
	unix, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("proactive: parse last proactive at %q: %w", raw, err)
	}
	return time.Unix(unix, 0), true, nil
}

// SetLastProactiveAt 更新会话最近一次主动发送时间。
//
// 冷却按会话维度记录，避免同一群或同一私聊被连续主动打扰。
func (s *State) SetLastProactiveAt(ctx context.Context, platform domain.Platform, sessionID string, at time.Time) error {
	if err := s.rdb.HSet(ctx, keyLastProactiveAt, makeSessionRef(platform, sessionID), at.Unix()).Err(); err != nil {
		return fmt.Errorf("proactive: set last proactive at: %w", err)
	}
	return nil
}

// DailyCount 读取 day 所在本地日期的主动发送次数。
//
// day 使用调用方传入的本地时间，和调度时间窗保持同一时区语义。
func (s *State) DailyCount(ctx context.Context, day time.Time) (int, error) {
	raw, err := s.rdb.Get(ctx, keyDailyCount(day)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("proactive: read daily count: %w", err)
	}
	count, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("proactive: parse daily count %q: %w", raw, err)
	}
	return count, nil
}

// IncrementDailyCount 累加本地日期维度的发送次数，并设置过期时间避免长期堆积。
//
// INCR 和 EXPIRE 放在 pipeline 里减少往返；偶发失败会由调度器返回错误并重试下一轮。
func (s *State) IncrementDailyCount(ctx context.Context, day time.Time) (int64, error) {
	key := keyDailyCount(day)
	pipe := s.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, dailyCountTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("proactive: increment daily count: %w", err)
	}
	return incr.Val(), nil
}

// PendingContext 保存刚主动发出的短期上下文。
//
// 下一条用户回复命中同一会话时，可以据此知道机器人刚刚主动说过什么；TTL 到期后
// 自动失效，避免把一次主动开场长期当成当前话题。
type PendingContext struct {
	Platform      domain.Platform         `json:"platform"`
	ConvType      domain.ConversationType `json:"conv_type"`
	SessionID     string                  `json:"session_id"`
	UserID        string                  `json:"user_id,omitempty"`
	Source        CandidateSource         `json:"source"`
	Text          string                  `json:"text"`
	CreatedAtUnix int64                   `json:"created_at_unix"`
}

// SetPendingContext 写入短 TTL 上下文，只服务下一轮可能的用户承接。
//
// 只有真实发送成功后才应调用；DryRun 不写 PendingContext，避免伪造对话状态。
func (s *State) SetPendingContext(ctx context.Context, pending PendingContext, ttl time.Duration) error {
	if pending.Platform == "" || pending.SessionID == "" {
		return fmt.Errorf("proactive: incomplete pending context")
	}
	if ttl <= 0 {
		ttl = defaultPendingTTL
	}
	data, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("proactive: marshal pending context: %w", err)
	}
	if err := s.rdb.Set(ctx, keyPendingPrefix+makeSessionRef(pending.Platform, pending.SessionID), data, ttl).Err(); err != nil {
		return fmt.Errorf("proactive: set pending context: %w", err)
	}
	return nil
}

// PendingContext 读取会话里的短期主动上下文。
//
// 缺失返回 ok=false，调用方可直接走普通入站处理。
func (s *State) PendingContext(ctx context.Context, platform domain.Platform, sessionID string) (PendingContext, bool, error) {
	raw, err := s.rdb.Get(ctx, keyPendingPrefix+makeSessionRef(platform, sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return PendingContext{}, false, nil
	}
	if err != nil {
		return PendingContext{}, false, fmt.Errorf("proactive: read pending context: %w", err)
	}
	var pending PendingContext
	if err := json.Unmarshal([]byte(raw), &pending); err != nil {
		return PendingContext{}, false, fmt.Errorf("proactive: parse pending context: %w", err)
	}
	return pending, true, nil
}

// ClearPendingContext 清理会话里的短期主动上下文。
//
// 用户已经承接或主动上下文被消费后应清理，避免下一条无关消息误接上旧话题。
func (s *State) ClearPendingContext(ctx context.Context, platform domain.Platform, sessionID string) error {
	if err := s.rdb.Del(ctx, keyPendingPrefix+makeSessionRef(platform, sessionID)).Err(); err != nil {
		return fmt.Errorf("proactive: clear pending context: %w", err)
	}
	return nil
}

// keyDailyCount 用本地日期生成日限额 key；调用方负责传入同一时区的 now。
func keyDailyCount(day time.Time) string {
	return keyDailyCountPrefix + day.Format("20060102")
}

// makeUserKey 与 stats.affinityField 的 member 形态保持一致。
func makeUserKey(platform domain.Platform, userID string) string {
	p := string(platform)
	if p == "" {
		p = "unknown"
	}
	return p + "_" + userID
}

// splitUserKey 解析 stats 好感度排行 member；格式不符时让调用方跳过。
func splitUserKey(key string) (domain.Platform, string, bool) {
	platform, userID, ok := strings.Cut(key, "_")
	if !ok || platform == "" || userID == "" {
		return "", "", false
	}
	return domain.Platform(platform), userID, true
}

// makeSessionRef 给会话元数据构造跨平台引用，分隔符避开 user key 的下划线格式。
func makeSessionRef(platform domain.Platform, sessionID string) string {
	return string(platform) + "|" + sessionID
}

// normalizeGroupSessionID 兼容管理命令只输入群号的情况，内部统一存 group_ 前缀。
func normalizeGroupSessionID(groupSessionID string) string {
	groupSessionID = strings.TrimSpace(groupSessionID)
	if groupSessionID == "" || strings.HasPrefix(groupSessionID, "group_") {
		return groupSessionID
	}
	return "group_" + groupSessionID
}
