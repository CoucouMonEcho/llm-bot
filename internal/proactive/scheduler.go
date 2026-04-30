// Package proactive 的 scheduler.go 实现主动消息调度循环。
//
// 调度器按固定间隔加随机抖动运行：先检查 Redis 运行期开关与时间窗，再扫
// 一遍"群最后活跃时间" HASH，挑出最久未活跃且超过冷却阈值的群，生成开场
// 白发出去，最后回写 last_inbound 防自激发。
//
// 决策面只剩"群冷却 + 时间窗 + Redis 开关"三件事——日限额、会话冷却、
// 好感度排行、白名单等已被刻意删除：群冷却本身已经是足够的频率约束，
// "群里 1h 没人说话才主动开口"在直觉上也容易解释。
package proactive

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/domain"
)

// sendTimeout 给发送链路独立超时，避免下游平台卡住整个调度循环。
const sendTimeout = 15 * time.Second

// Config 是主动消息调度器的静态配置。
//
// WindowStart/WindowEnd 使用本地时间的 HH:MM 字符串；运行期开关保存在 Redis
// （`State.Enabled`），由 RunOnce 每轮检查，本结构体只保存调度参数。
//
// Generator 关心的字段也平铺在这里——proactive 内部组件都从同一份 Config
// 取自己关心的子集，避免再嵌套一层只为了"按职责分组"。
type Config struct {
	WindowStart   string
	WindowEnd     string
	Interval      time.Duration
	Jitter        time.Duration
	IdleThreshold time.Duration

	HistorySize     int
	MaxHistoryChars int
}

// Sender 是调度器依赖的最小发送接口。
//
// 调度器只关心"把一条 OutboundMessage 发出去"，具体平台适配由 bot 层实现。
type Sender interface {
	Send(ctx context.Context, out *domain.OutboundMessage) error
}

// HistoryWriter 是调度器写入历史所需的最小接口。
//
// store.HistoryRepo 天然满足这个接口；这里收窄依赖面，避免 Scheduler 需要关心
// 历史读取能力。
type HistoryWriter interface {
	Append(ctx context.Context, sessionID string, msg *schema.Message, maxLen int) error
}

// Options 汇总 Scheduler 的依赖与配置。
//
// State/Generator/Sender 都是必需依赖；NewScheduler 不立即校验，RunOnce
// 会在真正执行时返回明确错误，方便测试按需替换其中一部分。
type Options struct {
	State      *State
	Generator  *Generator
	Sender     Sender
	History    HistoryWriter
	HistoryMax int
	Logger     *slog.Logger
	Config     Config
}

// Scheduler 运行单进程的主动消息调度循环。
//
// 这里不做分布式锁或主节点选举；多实例同时 Run 时可能在同一群上重复发送。
// 当前约束是由进程装配层只启动一个 Scheduler。
type Scheduler struct {
	state      *State
	generator  *Generator
	sender     Sender
	history    HistoryWriter
	historyMax int
	log        *slog.Logger
	cfg        Config
	now        func() time.Time
	rng        *rand.Rand
}

// NewScheduler 构造调度器；这里不做分布式锁，调用方需保证只启动一个实例。
func NewScheduler(opts Options) *Scheduler {
	cfg := opts.Config
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Minute
	}
	return &Scheduler{
		state:      opts.State,
		generator:  opts.Generator,
		sender:     opts.Sender,
		history:    opts.History,
		historyMax: opts.HistoryMax,
		log:        cmp.Or(opts.Logger, slog.Default()),
		cfg:        cfg,
		now:        time.Now,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run 持续执行调度，直到 ctx 取消。
//
// 单轮失败只记录日志，下一轮仍按 interval+jitter 继续尝试；主动消息是附加
// 能力，不应该因为一次扫描/生成/发送失败让后台循环退出。
func (s *Scheduler) Run(ctx context.Context) {
	if s == nil {
		return
	}
	for {
		if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("proactive scheduler iteration failed", "err", err)
		}

		delay := nextDelay(s.cfg.Interval, s.cfg.Jitter, s.rng.Int63n)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// RunOnce 执行一轮调度；生产路径通常由 Run 调用，测试和管理命令可直接使用。
//
// 流程：开关 → 时间窗 → 扫 hash → 选最旧 → 生成 → 发 → 写历史 → 回写 last_inbound。
// 任意短路点都会直接 return nil；历史写入失败只记录 warn，不阻断防自激发回写。
// 本轮至多发一个群，下轮再处理其他群——避免一次循环里把所有沉寂群轮一遍。
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("proactive: nil scheduler")
	}
	if s.state == nil || s.generator == nil || s.sender == nil {
		return fmt.Errorf("proactive: incomplete scheduler dependencies")
	}

	now := s.now()

	enabled, err := s.state.Enabled(ctx)
	if err != nil {
		return err
	}
	if !enabled {
		s.log.Debug("proactive scheduler skipped", "reason", "runtime_disabled")
		return nil
	}

	inWindow, err := inTimeWindow(now, s.cfg.WindowStart, s.cfg.WindowEnd)
	if err != nil {
		return err
	}
	if !inWindow {
		s.log.Debug("proactive scheduler skipped", "reason", "outside_time_window", "now", now)
		return nil
	}

	groups, err := s.state.GroupsLastInbound(ctx)
	if err != nil {
		return err
	}
	sessionID, lastAt, ok := pickOldestIdle(groups, now, s.cfg.IdleThreshold)
	if !ok {
		s.log.Debug("proactive scheduler skipped", "reason", "no_idle_group")
		return nil
	}

	text, err := s.generator.Generate(ctx, sessionID, lastAt, now)
	if err != nil {
		return err
	}

	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	if err := s.sender.Send(sendCtx, &domain.OutboundMessage{
		Platform:  domain.PlatformOneBot,
		ConvType:  domain.ConversationGroup,
		SessionID: sessionID,
		Text:      text,
	}); err != nil {
		return fmt.Errorf("proactive: send: %w", err)
	}

	if s.history != nil {
		if err := s.history.Append(ctx, sessionID, schema.AssistantMessage(text, nil), s.historyMax); err != nil {
			s.log.Warn("append proactive assistant history failed",
				"session", sessionID,
				"err", err)
		}
	}

	// 防自激发：发完即视作"群里和 bot 又互动了一次"，等同一条群消息进来。
	if err := s.state.RecordGroupInbound(ctx, sessionID, now); err != nil {
		return err
	}

	s.log.Info("proactive message sent",
		"session", sessionID,
		"idle", now.Sub(lastAt))
	return nil
}

// pickOldestIdle 从 last 中挑出 lastAt + idle <= now 的群里 lastAt 最小的那个。
//
// 全部未达 idle 时返回 ok=false；多群同 lastAt 时优先返回 sessionID 字典序
// 最小的（确定性可重复）。这是个纯函数：不依赖 *Scheduler、不依赖 ctx，
// 也不依赖任何随机源——便于表驱动单测覆盖边界（空 map / 全部未达 / 多群同
// lastAt / 边界等于 idle / now 早于 lastAt 等）。
func pickOldestIdle(last map[string]time.Time, now time.Time, idle time.Duration) (sessionID string, lastAt time.Time, ok bool) {
	for sid, t := range last {
		if now.Sub(t) < idle {
			continue
		}
		switch {
		case !ok:
			sessionID, lastAt, ok = sid, t, true
		case t.Before(lastAt):
			sessionID, lastAt = sid, t
		case t.Equal(lastAt) && sid < sessionID:
			sessionID = sid
		}
	}
	return sessionID, lastAt, ok
}

// inTimeWindow 判断 now 是否落在本地 HH:MM 时间窗内，支持跨零点窗口。
func inTimeWindow(now time.Time, startHHMM, endHHMM string) (bool, error) {
	start, err := parseHHMM(startHHMM)
	if err != nil {
		return false, fmt.Errorf("proactive: parse window start: %w", err)
	}
	end, err := parseHHMM(endHHMM)
	if err != nil {
		return false, fmt.Errorf("proactive: parse window end: %w", err)
	}
	current := now.Hour()*60 + now.Minute()
	if start == end {
		return true, nil
	}
	if start < end {
		return current >= start && current < end, nil
	}
	// start 晚于 end 表示时间窗跨过零点，例如 10:00-01:00。
	return current >= start || current < end, nil
}

// parseHHMM 把 HH:MM 解析为当天分钟数，供时间窗比较使用。
func parseHHMM(value string) (int, error) {
	hourRaw, minuteRaw, ok := strings.Cut(strings.TrimSpace(value), ":")
	if !ok {
		return 0, fmt.Errorf("invalid HH:MM %q", value)
	}
	hour, err := strconv.Atoi(hourRaw)
	if err != nil {
		return 0, fmt.Errorf("invalid hour %q: %w", hourRaw, err)
	}
	minute, err := strconv.Atoi(minuteRaw)
	if err != nil {
		return 0, fmt.Errorf("invalid minute %q: %w", minuteRaw, err)
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return 0, fmt.Errorf("time out of range %q", value)
	}
	return hour*60 + minute, nil
}

// nextDelay 计算下一轮等待时间；jitter 为 [0, jitter] 的正向随机抖动。
//
// 只向后加抖动，避免比配置 interval 更频繁地触发主动发送。
// interval 由 NewScheduler 兜底，这里直接信任入参。
func nextDelay(interval, jitter time.Duration, int63n func(int64) int64) time.Duration {
	if jitter <= 0 || int63n == nil {
		return interval
	}
	maxJitter := jitter.Nanoseconds()
	if maxJitter <= 0 {
		return interval
	}
	return interval + time.Duration(int63n(maxJitter+1))
}
