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
	//   - 运行期开关由外部运维写入，本包只读；
	//   - 群白名单同样由外部写入，本包只读；
	//   - 用户/会话索引用于从好感度用户回找可触达会话；
	//   - 近期群活动只记录白名单群的入站消息，供兜底策略挑选；
	//   - 最近主动发送时间和日计数负责冷却、日限额。
	//
	// 好感度排行不在本包维护：通过 stats.Store.TopUsers 读取 stats 模块的 ZSET，
	// 避免两个包同时拥有好感度语义。
	keyEnabled           = "bot_proactive_enabled"
	keyGroupWhitelist    = "bot_proactive_group_whitelist"
	keyUserLastInbound   = "bot_proactive_user_last_inbound"
	keyRecentGroupEvents = "bot_proactive_whitelist_group_events"
	keyLastProactiveAt   = "bot_proactive_last_proactive_at"

	keyUserSessionsPrefix = "bot_proactive_user_sessions_"
	keySessionMetaPrefix  = "bot_proactive_session_meta_"
	keyDailyCountPrefix   = "bot_proactive_daily_count_"

	// 近期群活动只保留最近一小段，避免 Redis List 随群聊流量无限增长。
	defaultRecentGroupEventCap = 200
	// SessionMeta 是可触达性的近似缓存；过期后由新的入站消息自然重建。
	sessionMetaTTL = 30 * 24 * time.Hour
	// 日计数横跨本地日期边界保留两天，给跨零点调度留出读取余量。
	dailyCountTTL = 48 * time.Hour
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

// makeSessionRef 给会话元数据构造跨平台引用。
//
// sessionID 自身已带 "private_" / "group_" 前缀，platform 在前再加下划线即可
// 区分。统一用 "_" 拼接和 makeUserKey 保持一致，方便排查时眼睛省事；
// 本包不会反向 split ref，所以不用担心与 user key 撞串——它们出现在不同的
// Redis 容器（user key 是 ZSET member / key 后缀，session ref 是 Hash field /
// Set member），值相等也不会被错配。
func makeSessionRef(platform domain.Platform, sessionID string) string {
	p := string(platform)
	if p == "" {
		p = "unknown"
	}
	return p + "_" + sessionID
}

// normalizeGroupSessionID 兼容管理命令只输入群号的情况，内部统一存 group_ 前缀。
func normalizeGroupSessionID(groupSessionID string) string {
	groupSessionID = strings.TrimSpace(groupSessionID)
	if groupSessionID == "" || strings.HasPrefix(groupSessionID, "group_") {
		return groupSessionID
	}
	return "group_" + groupSessionID
}
