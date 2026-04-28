// Package proactive 的 selector.go 实现主动消息候选选择。
//
// 策略分两层：先从 stats 好感度排行里找近期活跃且可触达的用户；没有合适目标时，
// 再从白名单群近期活动里挑一个可发送会话。选择失败返回 nil，不代表系统错误。
package proactive

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/echo/llm-bot/internal/domain"
)

const (
	// 默认只看好感度前 50，避免每轮调度扫完整个排行。
	defaultAffinityTopN = 50
	// 用户刚说完不立刻主动打扰，沉寂太久也不贸然唤起。
	defaultMinSince = time.Hour
	defaultMaxSince = 6 * time.Hour
	// 群活动兜底只扫描最近一小段，和记录器侧列表上限配合使用。
	defaultRecentScan = 100
	// 默认不额外加会话冷却，由调度间隔和日限额先兜住发送频率。
	defaultCooldownSlack = 0
)

// SelectorConfig 控制主动消息候选的选择范围与冷却规则。
//
// MinSince/MaxSince 只约束好感度用户的最近主动发言时间；RecentEventScan 服务群活动
// 兜底策略；SessionCooldown 对两类候选都生效。
type SelectorConfig struct {
	AffinityTopN    int
	MinSince        time.Duration
	MaxSince        time.Duration
	RecentEventScan int
	SessionCooldown time.Duration
}

// DefaultSelectorConfig 返回当前阶段保守的候选选择默认值。
//
// 默认值偏向“少打扰”：只触达一到六小时内出现过的人，且优先复用已有好感度信号。
func DefaultSelectorConfig() SelectorConfig {
	return SelectorConfig{
		AffinityTopN:    defaultAffinityTopN,
		MinSince:        defaultMinSince,
		MaxSince:        defaultMaxSince,
		RecentEventScan: defaultRecentScan,
		SessionCooldown: defaultCooldownSlack,
	}
}

// withDefaults 补齐未配置字段；SessionCooldown 允许 0，表示不启用额外冷却。
func (c SelectorConfig) withDefaults() SelectorConfig {
	d := DefaultSelectorConfig()
	if c.AffinityTopN <= 0 {
		c.AffinityTopN = d.AffinityTopN
	}
	if c.MinSince <= 0 {
		c.MinSince = d.MinSince
	}
	if c.MaxSince <= 0 {
		c.MaxSince = d.MaxSince
	}
	if c.RecentEventScan <= 0 {
		c.RecentEventScan = d.RecentEventScan
	}
	return c
}

// CandidateSource 标记候选来自哪一路选择策略。
//
// 后续写 PendingContext 时会保留来源，方便下一轮承接时知道这次主动开场的依据。
type CandidateSource string

const (
	// CandidateSourceAffinity 表示候选来自好感度排行。
	CandidateSourceAffinity CandidateSource = "affinity"
	// CandidateSourceRecentGroup 表示候选来自白名单群的近期活动。
	CandidateSourceRecentGroup CandidateSource = "recent_group"
)

// Candidate 是生成器和调度器共同使用的最小发送目标。
//
// 它只包含生成一条主动开场白和发送消息所需的信息；不携带完整历史，历史由
// Generator 按 SessionID 单独读取。
type Candidate struct {
	Platform      domain.Platform
	ConvType      domain.ConversationType
	SessionID     string
	UserID        string
	UserName      string
	Source        CandidateSource
	Affinity      float64
	LastInboundAt time.Time
	EventText     string
	EventAt       time.Time
}

// Selector 按两阶段策略挑选一个主动消息候选。
//
// 第一阶段优先从好感度用户回找可触达会话；失败后再看白名单群的近期活动。
// 某一路 Redis 读取失败会被记录并尽量尝试下一路，避免主动功能的局部故障放大。
type Selector struct {
	state *State
	log   *slog.Logger
	cfg   SelectorConfig
}

// NewSelector 构造基于 Redis 状态的候选选择器。
func NewSelector(state *State, log *slog.Logger, cfg SelectorConfig) *Selector {
	return &Selector{
		state: state,
		log:   cmp.Or(log, slog.Default()),
		cfg:   cfg.withDefaults(),
	}
}

// Select 执行两阶段候选选择；返回 nil 表示当前没有合适目标。
//
// 好感度策略失败时会降级尝试近期群活动；近期群活动自身失败才返回 error。
func (s *Selector) Select(ctx context.Context, now time.Time) (*Candidate, error) {
	if s == nil || s.state == nil {
		return nil, fmt.Errorf("proactive: nil selector state")
	}
	if cand, err := s.selectByAffinity(ctx, now); err != nil {
		s.log.Warn("proactive affinity selection failed", "err", err)
	} else if cand != nil {
		return cand, nil
	}
	return s.selectByRecentGroup(ctx, now)
}

// selectByAffinity 从好感度排行挑用户，再反查这个用户最近出现过的可发送会话。
func (s *Selector) selectByAffinity(ctx context.Context, now time.Time) (*Candidate, error) {
	entries, err := s.state.TopAffinity(ctx, s.cfg.AffinityTopN)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Score <= 0 {
			continue
		}
		// 好感度只决定“优先看谁”，还必须满足最近主动发言时间窗。
		lastInbound, ok, err := s.state.UserLastInbound(ctx, entry.Platform, entry.UserID)
		if err != nil {
			s.log.Warn("proactive last inbound read failed",
				"platform", entry.Platform, "userID", entry.UserID, "err", err)
			continue
		}
		if !ok || !withinLastInboundWindow(lastInbound, now, s.cfg.MinSince, s.cfg.MaxSince) {
			continue
		}

		sessions, err := s.state.UserSessions(ctx, entry.Platform, entry.UserID)
		if err != nil {
			s.log.Warn("proactive user sessions read failed",
				"platform", entry.Platform, "userID", entry.UserID, "err", err)
			continue
		}
		groupAllowed, unavailable := s.sessionEligibility(ctx, sessions, now)
		session, ok := chooseAffinitySession(sessions, groupAllowed, unavailable)
		if !ok {
			continue
		}
		return &Candidate{
			Platform:      session.Platform,
			ConvType:      session.ConvType,
			SessionID:     session.SessionID,
			UserID:        entry.UserID,
			UserName:      session.LastUserName,
			Source:        CandidateSourceAffinity,
			Affinity:      entry.Score,
			LastInboundAt: lastInbound,
		}, nil
	}
	return nil, nil
}

// selectByRecentGroup 从白名单群近期活动中挑一个不在冷却期的群。
func (s *Selector) selectByRecentGroup(ctx context.Context, now time.Time) (*Candidate, error) {
	events, err := s.state.RecentGroupEvents(ctx, s.cfg.RecentEventScan)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		// 第二阶段只从白名单群里挑近期活动，避免向未授权群主动开口。
		allowed, err := s.state.GroupWhitelisted(ctx, event.SessionID)
		if err != nil {
			s.log.Warn("proactive group whitelist check failed",
				"platform", event.Platform, "session", event.SessionID, "err", err)
			continue
		}
		if !allowed {
			continue
		}
		meta := SessionMeta{
			Platform:  event.Platform,
			ConvType:  domain.ConversationGroup,
			SessionID: event.SessionID,
		}
		if unavailable, err := s.sessionInCooldown(ctx, meta, now); err != nil {
			s.log.Warn("proactive cooldown check failed",
				"platform", event.Platform, "session", event.SessionID, "err", err)
			continue
		} else if unavailable {
			continue
		}
		return &Candidate{
			Platform:  event.Platform,
			ConvType:  domain.ConversationGroup,
			SessionID: event.SessionID,
			UserID:    event.UserID,
			UserName:  event.UserName,
			Source:    CandidateSourceRecentGroup,
			EventText: event.Text,
			EventAt:   event.at(),
		}, nil
	}
	return nil, nil
}

// sessionEligibility 批量计算会话是否群白名单通过、是否因冷却不可用。
//
// 结果拆成两个 map 是为了让 chooseAffinitySession 保持纯函数，便于单独测试排序规则。
func (s *Selector) sessionEligibility(ctx context.Context, sessions []SessionMeta, now time.Time) (map[string]bool, map[string]bool) {
	groupAllowed := make(map[string]bool, len(sessions))
	unavailable := make(map[string]bool, len(sessions))
	for _, session := range sessions {
		if session.ConvType == domain.ConversationGroup {
			allowed, err := s.state.GroupWhitelisted(ctx, session.SessionID)
			if err != nil {
				s.log.Warn("proactive group whitelist check failed",
					"platform", session.Platform, "session", session.SessionID, "err", err)
			}
			if allowed {
				groupAllowed[session.ref()] = true
			}
		}
		inCooldown, err := s.sessionInCooldown(ctx, session, now)
		if err != nil {
			s.log.Warn("proactive cooldown check failed",
				"platform", session.Platform, "session", session.SessionID, "err", err)
			unavailable[session.ref()] = true
			continue
		}
		if inCooldown {
			unavailable[session.ref()] = true
		}
	}
	return groupAllowed, unavailable
}

// sessionInCooldown 判断会话是否仍在主动发送冷却期内。
func (s *Selector) sessionInCooldown(ctx context.Context, session SessionMeta, now time.Time) (bool, error) {
	if s.cfg.SessionCooldown <= 0 {
		return false, nil
	}
	last, ok, err := s.state.LastProactiveAt(ctx, session.Platform, session.SessionID)
	if err != nil || !ok {
		return false, err
	}
	return now.Sub(last) < s.cfg.SessionCooldown, nil
}

// withinLastInboundWindow 判断用户最近活跃是否落在“适合主动打扰”的时间窗内。
func withinLastInboundWindow(last, now time.Time, minSince, maxSince time.Duration) bool {
	if last.IsZero() || now.IsZero() || minSince < 0 || maxSince < minSince || last.After(now) {
		return false
	}
	age := now.Sub(last)
	return age >= minSince && age <= maxSince
}

// chooseAffinitySession 在同一用户的多个会话中选目标。
//
// 先按最近出现时间排序，再优先选择已白名单的群聊；没有可用群聊时才退到私聊。
// 这样可以把“主动开口”尽量放在用户最近出现的公共上下文里。
func chooseAffinitySession(sessions []SessionMeta, groupAllowed map[string]bool, unavailable map[string]bool) (SessionMeta, bool) {
	if len(sessions) == 0 {
		return SessionMeta{}, false
	}
	ordered := append([]SessionMeta(nil), sessions...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].LastSeenUnix > ordered[j].LastSeenUnix
	})

	for _, session := range ordered {
		if session.ConvType != domain.ConversationGroup || unavailable[session.ref()] {
			continue
		}
		if groupAllowed[session.ref()] {
			return session, true
		}
	}
	for _, session := range ordered {
		if session.ConvType == domain.ConversationPrivate && !unavailable[session.ref()] {
			return session, true
		}
	}
	return SessionMeta{}, false
}
