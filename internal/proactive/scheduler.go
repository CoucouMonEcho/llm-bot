// Package proactive 的 scheduler.go 实现主动消息调度循环。
//
// 调度器按固定间隔加随机抖动运行：先检查配置开关和 Redis 运行期开关，再判断时间窗、
// 日限额、候选、生成和发送。发送成功后才写冷却、日计数。
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

	"github.com/echo/llm-bot/internal/domain"
)

// sendTimeout 给发送链路独立超时，避免下游平台卡住整个调度循环。
const sendTimeout = 15 * time.Second

// Config 是主动消息调度器的静态配置。
//
// WindowStart/WindowEnd 使用本地时间的 HH:MM 字符串；Enabled 只代表配置侧开关，
// 还必须同时打开 Redis 运行期开关才会真正发送。DryRun 只生成和记录日志，不写发送状态。
//
// Selector / Generator 关心的字段直接平铺在这里——proactive 内部的几个组件
// 都从同一份 Config 取自己关心的子集，避免再嵌套一层只为了"按职责分组"。
type Config struct {
	Enabled     bool
	WindowStart string
	WindowEnd   string
	Interval    time.Duration
	Jitter      time.Duration
	DailyLimit  int
	DryRun      bool

	// 选择器相关
	AffinityTopN    int
	MinSinceLast    time.Duration
	MaxSinceLast    time.Duration
	RecentEventScan int
	SessionCooldown time.Duration

	// 生成器相关
	HistorySize     int
	MaxHistoryChars int
}

// Sender 是调度器依赖的最小发送接口。
//
// 调度器只关心“把一条 OutboundMessage 发出去”，具体平台适配由 bot 层实现。
type Sender interface {
	Send(ctx context.Context, out *domain.OutboundMessage) error
}

// Options 汇总 Scheduler 的依赖与配置。
//
// State/Selector/Generator/Sender 都是必需依赖；NewScheduler 不立即校验，
// RunOnce 会在真正执行时返回明确错误，方便测试按需替换其中一部分。
type Options struct {
	State     *State
	Selector  *Selector
	Generator *Generator
	Sender    Sender
	Logger    *slog.Logger
	Config    Config
}

// Scheduler 运行单进程的主动消息调度循环。
//
// 这里不做分布式锁或主节点选举；如果多实例同时 Run，日限额和冷却可能出现
// 竞争窗口。当前约束是由进程装配层只启动一个 Scheduler。
type Scheduler struct {
	state     *State
	selector  *Selector
	generator *Generator
	sender    Sender
	log       *slog.Logger
	cfg       Config
	now       func() time.Time
	rng       *rand.Rand
}

// NewScheduler 构造调度器；这里不做分布式锁，调用方需保证只启动一个实例。
func NewScheduler(opts Options) *Scheduler {
	cfg := opts.Config
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	return &Scheduler{
		state:     opts.State,
		selector:  opts.Selector,
		generator: opts.Generator,
		sender:    opts.Sender,
		log:       cmp.Or(opts.Logger, slog.Default()),
		cfg:       cfg,
		now:       time.Now,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Run 持续执行调度，直到 ctx 取消。
//
// 单轮失败只记录日志，下一轮仍按 interval+jitter 继续尝试；主动消息是附加能力，
// 不应该因为一次候选/生成/发送失败让后台循环退出。
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
// 静态开关、运行期开关、时间窗、日限额都会在生成前短路。只有真实发送成功后才写
// 最近主动发送时间和日计数，保证这些状态与实际发出的消息一致。
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("proactive: nil scheduler")
	}
	if s.state == nil || s.selector == nil || s.generator == nil || s.sender == nil {
		return fmt.Errorf("proactive: incomplete scheduler dependencies")
	}

	now := s.now()
	if !s.cfg.Enabled {
		s.log.Debug("proactive scheduler skipped", "reason", "static_disabled")
		return nil
	}

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

	count, err := s.state.DailyCount(ctx, now)
	if err != nil {
		return err
	}
	if count >= s.cfg.DailyLimit {
		s.log.Info("proactive scheduler skipped", "reason", "daily_limit", "count", count, "limit", s.cfg.DailyLimit)
		return nil
	}

	cand, err := s.selector.Select(ctx, now)
	if err != nil {
		return err
	}
	if cand == nil {
		s.log.Debug("proactive scheduler skipped", "reason", "no_candidate")
		return nil
	}

	text, err := s.generator.Generate(ctx, *cand, now)
	if err != nil {
		return err
	}
	if s.cfg.DryRun {
		// DryRun 只产生日志，不发送消息，也不写入冷却或日限额。
		s.log.Info("proactive dry-run would send",
			"platform", cand.Platform,
			"convType", cand.ConvType,
			"session", cand.SessionID,
			"source", cand.Source,
			"text", text)
		return nil
	}

	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	if err := s.sender.Send(sendCtx, &domain.OutboundMessage{
		Platform:  cand.Platform,
		ConvType:  cand.ConvType,
		SessionID: cand.SessionID,
		Text:      text,
		ReplyTo:   nil,
	}); err != nil {
		return fmt.Errorf("proactive: send message: %w", err)
	}

	// 只有真实发送成功后才写状态：冷却时间和日限额必须保持一致。
	if err := s.state.SetLastProactiveAt(ctx, cand.Platform, cand.SessionID, now); err != nil {
		return err
	}
	if _, err := s.state.IncrementDailyCount(ctx, now); err != nil {
		return err
	}

	s.log.Info("proactive message sent",
		"platform", cand.Platform,
		"convType", cand.ConvType,
		"session", cand.SessionID,
		"source", cand.Source)
	return nil
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
