// 定时拉取洛克王国"远行商人"接口、过滤本轮限时新商品，通过共享 OneBot Adapter 推送。
//
// 设计为 llm-bot 内部后台任务：
//   - 复用 llm-bot 已建立的 OneBot 反向 WebSocket，不单独连接 NapCat；
//   - 远行商人接口与凭证硬编码在文件顶部，调参只看一处；
//   - 推送目标存放在同级目录 rocom.yaml，每次发送前热读，运行中改 yaml 立即生效；
//   - 状态全内存（仅记录"本轮已推过"），进程重启即丢失，同一轮次有可能被重复推送一次。
//
// 调度策略：
//
//	8:00 / 12:00 / 16:00 / 20:00 整点进入 30 分钟重试窗口；
//	每 60 秒拉一次接口，命中本轮限时新商品后推送并跳出，否则窗口耗尽放弃本轮。
package rocom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/echo/llm-bot/internal/domain"
	"gopkg.in/yaml.v3"
)

const (
	// apiURL 是远行商人接口；refresh=true 强制取最新库存。
	apiURL = "https://wegame.shallow.ink/api/v1/games/rocom/merchant/info?refresh=true"
	// apiKey 是 wegame.shallow.ink 接口的 X-API-Key 凭证。
	apiKey = "sk-ba042e079cf9ccb30e72b3d5af458f45"

	httpTimeout = 15 * time.Second
	sendTimeout = 15 * time.Second

	// 整点开始后，每 retryInterval 拉一次接口，最多持续 retryWindow，
	// 命中本轮新商品立即推送并停止。
	retryInterval = 60 * time.Second

	settingsName = "rocom.yaml"
)

var httpClient = &http.Client{Timeout: httpTimeout}

// Sender 是本任务依赖的最小发送接口；onebot.Adapter 天然满足它。
type Sender interface {
	Send(ctx context.Context, out *domain.OutboundMessage) error
}

// Run 启动远行商人后台轮询任务，直到 ctx 取消。
func Run(ctx context.Context, sender Sender, logger *slog.Logger) {
	if sender == nil {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	log := logger.With(slog.String("component", "rocom"))
	log.Info("rocom merchant started")

	// lastPushedRound 记录进程内最近一次已成功推送的轮次（1~4）；
	// 仅用于在同一轮次的重试循环中标记"已完成"，进程重启即清零。
	var lastPushedRound int

	for {
		if ctx.Err() != nil {
			log.Info("rocom merchant stopped")
			return
		}

		now := time.Now()
		// 一天分四个轮次：8~12 → 1，12~16 → 2，16~20 → 3，20~24 → 4；其余为 0。
		h := now.Hour()
		round := 0
		if h >= 8 && h < 24 {
			round = (h-8)/4 + 1
		}

		switch {
		case round == 0:
			// 非活动时段（0~8 点）。
			if !sleepUntilNextRound(ctx, log) {
				return
			}
		case round == lastPushedRound:
			// 本轮已推过，直接等下一轮整点。
			if !sleepUntilNextRound(ctx, log) {
				return
			}
		// 不启用 retryWindow 截断，重启后方便测试
		default:
			// 在重试窗口内：尝试一次拉取并推送。
			if runOnce(ctx, sender, log, round) {
				log.Info("rocom merchant round pushed", slog.Int("round", round))
				lastPushedRound = round
				if !sleepUntilNextRound(ctx, log) {
					return
				}
			} else if !sleep(ctx, retryInterval) {
				return
			}
		}
	}
}

// sleepUntilNextRound 计算到下一个 8/12/16/20 整点的距离并休眠。
func sleepUntilNextRound(ctx context.Context, log *slog.Logger) bool {
	next, round := nextRoundStart(time.Now())
	d := time.Until(next)
	log.Info("rocom merchant sleeping until next round",
		slog.Int("round", round),
		slog.String("next", next.Format("2006-01-02 15:04:05")),
		slog.Duration("after", d.Truncate(time.Second)))
	return sleep(ctx, d)
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// runOnce 跑一次"拉接口 → 过滤 → 推送"链路。
//
// 返回 true 表示本轮已发现限时新商品，且至少一个目标推送成功；
// 调用方据此标记本轮完成、跳到下一轮等待。
// 返回 false 表示接口失败、未发现新商品或没有目标推送成功，调用方应稍后重试。
func runOnce(ctx context.Context, sender Sender, log *slog.Logger, round int) bool {
	data, err := fetchMerchant(ctx)
	if err != nil {
		log.Warn("rocom merchant api fetch failed", slog.Any("err", err))
		return false
	}
	now := time.Now()
	props := freshProps(data, round, now)
	if len(props) == 0 {
		log.Info("rocom merchant no fresh props", slog.Int("round", round))
		return false
	}
	names := propNames(props)
	log.Info("rocom merchant fresh props found",
		slog.Int("round", round),
		slog.String("props", strings.Join(names, "、")))

	return pushAll(ctx, sender, log, fmt.Sprintf("远行商人推送通知：%s",
		strings.Join(names, "、"),
	))
}

// ---------- 调度：轮次时间计算 ----------

// roundStart 返回 day 当天 round 轮次（1~4）的开始整点；round 越界返回零值。
func roundStart(day time.Time, round int) time.Time {
	if round < 1 || round > 4 {
		return time.Time{}
	}
	hour := (round-1)*4 + 8
	return time.Date(day.Year(), day.Month(), day.Day(), hour, 0, 0, 0, day.Location())
}

// nextRoundStart 返回 now 之后下一个 8/12/16/20 整点和对应轮次。
// 当天 20:00 已过则返回次日 8:00（轮次 1）。
func nextRoundStart(now time.Time) (time.Time, int) {
	for r := 1; r <= 4; r++ {
		start := roundStart(now, r)
		if start.After(now) {
			return start, r
		}
	}
	tomorrow := now.Add(24 * time.Hour)
	return roundStart(tomorrow, 1), 1
}

// ---------- API 拉取 ----------

type apiResp struct {
	Code    int     `json:"code"`
	Message string  `json:"message"`
	Data    apiData `json:"data"`
}

type apiData struct {
	MerchantActivities []apiActivity `json:"merchantActivities"`
}

type apiActivity struct {
	GetProps []apiProp `json:"get_props"`
}

type apiProp struct {
	Name      string `json:"name"`
	StartTime int64  `json:"start_time"` // 毫秒
	EndTime   int64  `json:"end_time"`   // 毫秒
}

// fetchMerchant 调一次远行商人接口并校验返回；失败时附带可读错误。
func fetchMerchant(ctx context.Context) (apiData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return apiData{}, err
	}
	req.Header.Set("X-API-Key", apiKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return apiData{}, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiData{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return apiData{}, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var r apiResp
	if err := json.Unmarshal(body, &r); err != nil {
		return apiData{}, fmt.Errorf("decode: %w", err)
	}
	if r.Code != 0 {
		return apiData{}, fmt.Errorf("api code=%d msg=%s", r.Code, r.Message)
	}
	return r.Data, nil
}

// ---------- 过滤 ----------

// freshProps 从 API 数据中挑出"在 now 时刻已上架、属于本轮新增、且非全天商品"的商品。
//
// 三段过滤（顺序固定）：
//  1. 在上架窗口内：start_time <= now <= end_time；
//  2. 不是全天商品：全天商品形如"start 早于 8:00 且 end 接近 23:59"，反向判定；
//  3. 是本轮新上架：start_time 不早于 round 开始时间前 5 分钟（兼容接口刷新抖动）。
func freshProps(data apiData, round int, now time.Time) []apiProp {
	rs := roundStart(now, round)
	nowMs := now.UnixMilli()
	var out []apiProp
	for _, act := range data.MerchantActivities {
		for _, p := range act.GetProps {
			if p.StartTime > nowMs || p.EndTime < nowMs {
				continue
			}
			if !isLimitedTime(p) {
				continue
			}
			if !isCurrentRound(p, rs) {
				continue
			}
			out = append(out, p)
		}
	}
	return out
}

// isLimitedTime 判断商品是否为限时商品（即非全天商品）。
//
// 全天商品的特征：开始时间早于当天 8:00，且结束时间为当天 23:50 之后；
// 满足以上两条视为全天，否则视为限时。
func isLimitedTime(p apiProp) bool {
	if p.StartTime <= 0 || p.EndTime <= 0 {
		return false
	}
	start := time.UnixMilli(p.StartTime)
	end := time.UnixMilli(p.EndTime)
	if start.Hour() < 8 && end.Hour() == 23 && end.Minute() >= 50 {
		return false
	}
	return true
}

// isCurrentRound 判断商品是否在 round 开始时间附近上架（容忍 5 分钟刷新抖动）。
func isCurrentRound(p apiProp, roundStart time.Time) bool {
	if p.StartTime <= 0 {
		return false
	}
	start := time.UnixMilli(p.StartTime)
	return !start.Before(roundStart.Add(-5 * time.Minute))
}

// propNames 取商品名列表并按出现顺序去重；空名字跳过。
func propNames(props []apiProp) []string {
	seen := make(map[string]struct{}, len(props))
	out := make([]string, 0, len(props))
	for _, p := range props {
		n := strings.TrimSpace(p.Name)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

// ---------- 推送：复用 OneBot 反向 WebSocket ----------

// pushAll 把文案推送给 rocom.yaml 中所有目标。
//
// 每次发送前都重读 rocom.yaml，修改 yaml 即时生效，无需重启进程。
// 单个目标失败仅记日志、不影响其他目标；至少一个目标发送成功时返回 true。
func pushAll(ctx context.Context, sender Sender, log *slog.Logger, text string) bool {
	s, err := loadSettings()
	if err != nil {
		log.Warn("rocom merchant settings load failed", slog.Any("err", err))
		return false
	}
	if len(s.Groups) == 0 && len(s.Privates) == 0 {
		log.Info("rocom merchant settings has no targets")
		return false
	}
	var sent bool
	for _, gid := range s.Groups {
		if pushOne(ctx, sender, log, fmt.Sprintf("群 %d", gid), &domain.OutboundMessage{
			Platform:  domain.PlatformOneBot,
			ConvType:  domain.ConversationGroup,
			SessionID: fmt.Sprintf("group_%d", gid),
			Text:      text,
		}) {
			sent = true
		}
	}
	for _, uid := range s.Privates {
		if pushOne(ctx, sender, log, fmt.Sprintf("私聊 %d", uid), &domain.OutboundMessage{
			Platform:  domain.PlatformOneBot,
			ConvType:  domain.ConversationPrivate,
			SessionID: fmt.Sprintf("private_%d", uid),
			Text:      text,
		}) {
			sent = true
		}
	}
	return sent
}

func pushOne(ctx context.Context, sender Sender, log *slog.Logger, target string, out *domain.OutboundMessage) bool {
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	if err := sender.Send(sendCtx, out); err != nil {
		log.Warn("rocom merchant push failed",
			slog.String("target", target),
			slog.Any("err", err))
		return false
	}
	log.Info("rocom merchant push succeeded", slog.String("target", target))
	return true
}

// ---------- rocom.yaml ----------

// settings 是 rocom.yaml 的结构；两个字段都允许缺省。
type settings struct {
	Groups   []int64 `yaml:"groups"`
	Privates []int64 `yaml:"privates"`
}

// loadSettings 每次发送前都重读 rocom.yaml，使运行中修改即时生效。
//
// 候选路径按优先级：
//  1. 可执行文件同级目录（go build 后部署最常见）；
//  2. 当前工作目录（直接 ./llm-bot 启动时）；
//  3. 仓库内 cmd/rocom-merchant/（go run ./cmd/llm-bot 时友好）。
//
// 命中第一个能读到的路径即返回；全部都不存在时返回 not found 错误。
func loadSettings() (settings, error) {
	var lastErr error
	for _, p := range candidateSettingsPaths() {
		data, err := os.ReadFile(p)
		if err == nil {
			var s settings
			if err := yaml.Unmarshal(data, &s); err != nil {
				return settings{}, fmt.Errorf("parse %s: %w", p, err)
			}
			return s, nil
		}
		if !os.IsNotExist(err) {
			return settings{}, fmt.Errorf("read %s: %w", p, err)
		}
		lastErr = err
	}
	return settings{}, fmt.Errorf("%s not found: %v", settingsName, lastErr)
}

func candidateSettingsPaths() []string {
	var paths []string
	if exe, err := os.Executable(); err == nil {
		paths = append(paths, filepath.Join(filepath.Dir(exe), settingsName))
	}
	paths = append(paths, settingsName)
	paths = append(paths, filepath.Join("configs", settingsName))
	return paths
}

// ---------- 杂项 ----------

// truncate 把长字符串截断到 n 字节并加省略标记，仅用于错误日志。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
