// Package proactive 的 recorder.go 实现"群入站消息旁路记录"。
//
// 它不参与回复生成，只把"群里最近一次和 bot 互动的时间"刷进 Redis，供
// Scheduler 选最久未活跃的群。私聊消息不在主动消息覆盖范围内，这里直接忽略。
// 写入失败只打日志，永远不阻断主对话链路。
package proactive

import (
	"cmp"
	"context"
	"log/slog"
	"time"

	"github.com/echo/llm-bot/internal/domain"
)

// ActivityRecorder 从入站消息维护"群最后互动时间" HASH。
//
// 它适合挂在 bot 收到消息后的旁路位置：先让正常回复链路继续走，再尽力刷新
// proactive 状态。记录失败不会向上返回错误。
type ActivityRecorder struct {
	state *State
	log   *slog.Logger
}

// NewActivityRecorder 构造入站活动记录器。
//
// 不再有 cap 之类的运行期参数——recorder 唯一要做的事就是 HSET 一条群最后
// 互动时间，没有"列表长度上限"这种语义可调。
func NewActivityRecorder(state *State, log *slog.Logger) *ActivityRecorder {
	return &ActivityRecorder{
		state: state,
		log:   cmp.Or(log, slog.Default()),
	}
}

// RecordInbound 把群消息记入"群最后互动时间"。
//
// Adapter 在源头已经过滤掉非 @bot 的群消息，所以走到这里的群消息必然是
// "群里有人和 bot 主动对话"——它正是判断"该群是否需要主动开口"的反例信号。
// 私聊不在主动消息覆盖范围内，这里直接 return；保留 nil/sessionID 缺失的
// 防御性短路，避免上游异常调用一路打 warn。
func (r *ActivityRecorder) RecordInbound(ctx context.Context, msg *domain.InboundMessage) {
	if r == nil || r.state == nil || msg == nil {
		return
	}
	if msg.ConvType != domain.ConversationGroup || msg.SessionID == "" {
		return
	}
	if err := r.state.RecordGroupInbound(ctx, msg.SessionID, time.Now()); err != nil {
		r.log.Warn("proactive record group inbound failed",
			"session", msg.SessionID, "err", err)
	}
}
