// Package proactive 的 recorder.go 实现“入站消息旁路记录”。
//
// 它不参与回复生成，只把用户最近活跃时间、用户可触达会话、白名单群近期活动写入
// Redis，供后续主动消息调度选择候选。所有写入失败都只打日志，不阻断主对话链路。
package proactive

import (
	"cmp"
	"context"
	"log/slog"
	"time"

	"github.com/echo/llm-bot/internal/domain"
)

// RecorderConfig 控制 ActivityRecorder 写入近期群活动的容量。
//
// 容量只影响近期群活动兜底策略的可见范围，不影响好感度优先策略。
type RecorderConfig struct {
	RecentGroupEventCap int
}

// ActivityRecorder 从入站消息维护主动候选所需的轻量索引。
//
// 它适合挂在 bot 收到消息后的旁路位置：先让正常回复链路继续走，再尽力刷新
// proactive 状态。记录失败不会向上返回错误。
type ActivityRecorder struct {
	state *State
	log   *slog.Logger
	cfg   RecorderConfig
	now   func() time.Time
}

// NewActivityRecorder 构造入站活动记录器。
//
// RecentGroupEventCap 未配置时使用默认上限，避免新接入时忘配导致 Redis List 膨胀。
func NewActivityRecorder(state *State, log *slog.Logger, cfg RecorderConfig) *ActivityRecorder {
	if cfg.RecentGroupEventCap <= 0 {
		cfg.RecentGroupEventCap = defaultRecentGroupEventCap
	}
	return &ActivityRecorder{
		state: state,
		log:   cmp.Or(log, slog.Default()),
		cfg:   cfg,
		now:   time.Now,
	}
}

// RecordInbound 记录用户活动与已知会话。
//
// 这里是回复链路的旁路写入：先记录“谁最近说过话”和“这个人在哪些会话可触达”，
// 再在群聊且命中白名单时保存近期群活动。任一步失败都只记日志，不阻断正常回复。
func (r *ActivityRecorder) RecordInbound(ctx context.Context, msg *domain.InboundMessage) {
	if r == nil || r.state == nil || msg == nil {
		return
	}
	if msg.UserID == "" || msg.Platform == "" || msg.SessionID == "" {
		return
	}

	at := r.now()
	if err := r.state.RecordUserLastInbound(ctx, msg.Platform, msg.UserID, at); err != nil {
		r.log.Warn("proactive record user last inbound failed",
			"platform", msg.Platform, "userID", msg.UserID, "err", err)
	}
	if err := r.state.RecordSession(ctx, msg, at); err != nil {
		r.log.Warn("proactive record session failed",
			"platform", msg.Platform, "session", msg.SessionID, "userID", msg.UserID, "err", err)
	}

	if msg.ConvType != domain.ConversationGroup {
		return
	}
	allowed, err := r.state.GroupWhitelisted(ctx, msg.SessionID)
	if err != nil {
		r.log.Warn("proactive group whitelist check failed",
			"platform", msg.Platform, "session", msg.SessionID, "err", err)
		return
	}
	if !allowed {
		return
	}

	event := RecentGroupEvent{
		Platform:  msg.Platform,
		SessionID: msg.SessionID,
		UserID:    msg.UserID,
		UserName:  msg.UserName,
		Text:      msg.Text,
		AtUnix:    at.Unix(),
	}
	if err := r.state.AddRecentGroupEvent(ctx, event, r.cfg.RecentGroupEventCap); err != nil {
		r.log.Warn("proactive record group event failed",
			"platform", msg.Platform, "session", msg.SessionID, "userID", msg.UserID, "err", err)
	}
}
