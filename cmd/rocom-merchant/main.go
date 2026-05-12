// Command rocom-merchant 独立工具：
// 定时拉取洛克王国"远行商人"接口、过滤本轮限时新商品、推送到 NapCat HTTP API。
//
// 设计为完全独立：
//   - 不 import 本仓库 internal/* 任何包；
//   - 接口、凭证、NapCat 地址硬编码在文件顶部，调参只看一处；
//   - 推送目标存放在同级目录 settings.yaml，每次发送前热读，运行中改 yaml 立即生效；
//   - 状态全内存（仅记录"本轮已推过"），进程重启即丢失，同一轮次有可能被重复推送一次。
//
// 调度策略：
//
//	8:00 / 12:00 / 16:00 / 20:00 整点进入 30 分钟重试窗口；
//	每 60 秒拉一次接口，命中本轮限时新商品后推送并跳出，否则窗口耗尽放弃本轮。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ---------- 顶部硬编码区 ----------

const (
	// apiURL 是远行商人接口；refresh=true 强制取最新库存。
	apiURL = "https://wegame.shallow.ink/api/v1/games/rocom/merchant/info?refresh=true"
	// apiKey 是 wegame.shallow.ink 接口的 X-API-Key 凭证；自行填入。
	apiKey = "sk-ba042e079cf9ccb30e72b3d5af458f45"
	// napcatBase 是本地 NapCatQQ HTTP API 的基地址；本地部署无需鉴权。
	napcatBase = "http://127.0.0.1:6099"

	httpTimeout = 15 * time.Second

	// 整点开始后，每 retryInterval 拉一次接口，最多持续 retryWindow，
	// 命中本轮新商品立即推送并停止；耗尽窗口则放弃本轮。
	retryInterval = 60 * time.Second
	retryWindow   = 30 * time.Minute

	settingsName = "settings.yaml"
)

var httpClient = &http.Client{Timeout: httpTimeout}

// ---------- 主循环 ----------

func main() {
	log.SetFlags(log.LstdFlags)
	log.Println("rocom-merchant 启动")

	// lastPushedRound 记录进程内最近一次已成功推送的轮次（1~4）；
	// 仅用于在同一轮次的重试循环中标记"已完成"，进程重启即清零。
	var lastPushedRound int

	for {
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
			sleepUntilNextRound()
		case round == lastPushedRound:
			// 本轮已推过，直接等下一轮整点。
			sleepUntilNextRound()
		case now.Sub(roundStart(now, round)) > retryWindow:
			// 已超出本轮 30 分钟重试窗口（典型情况：在中段时刻启动），放弃本轮。
			log.Printf("当前时间已超出轮次 %d/4 的 %s 重试窗口，跳过", round, retryWindow)
			sleepUntilNextRound()
		default:
			// 在重试窗口内：尝试一次拉取并推送。
			if runOnce(round) {
				log.Printf("轮次 %d/4 推送完成", round)
				lastPushedRound = round
				sleepUntilNextRound()
			} else {
				time.Sleep(retryInterval)
			}
		}
	}
}

// sleepUntilNextRound 计算到下一个 8/12/16/20 整点的距离并休眠。
func sleepUntilNextRound() {
	next, round := nextRoundStart(time.Now())
	d := time.Until(next)
	log.Printf("休眠到下一轮 %d/4 开始：%s（约 %s 后）",
		round, next.Format("2006-01-02 15:04:05"), d.Truncate(time.Second))
	time.Sleep(d)
}

// runOnce 跑一次"拉接口 → 过滤 → 推送"链路。
//
// 返回 true 表示本轮已发现限时新商品并执行了推送（不论各目标单点成败），
// 调用方据此标记本轮完成、跳到下一轮等待；
// 返回 false 表示接口失败或未发现新商品，调用方应稍后重试。
func runOnce(round int) bool {
	data, err := fetchMerchant()
	if err != nil {
		log.Printf("API 拉取失败：%v", err)
		return false
	}
	now := time.Now()
	props := freshProps(data, round, now)
	if len(props) == 0 {
		log.Printf("接口正常，但未发现轮次 %d/4 的限时新商品", round)
		return false
	}
	names := propNames(props)
	log.Printf("发现轮次 %d/4 限时新商品：%s", round, strings.Join(names, "、"))

	pushAll(fmt.Sprintf("远行商人上新！\n商品：%s\n检测时间：%s",
		strings.Join(names, "、"),
		now.Format("2006-01-02 15:04:05"),
	))
	return true
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
func fetchMerchant() (apiData, error) {
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
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

// ---------- 推送：NapCat HTTP API ----------

// pushAll 把文案推送给 settings.yaml 中所有目标。
//
// 每次发送前都重读 settings.yaml，修改 yaml 即时生效，无需重启进程。
// 单个目标失败仅记日志、不影响其他目标。
func pushAll(text string) {
	s, err := loadSettings()
	if err != nil {
		log.Printf("加载 settings.yaml 失败：%v；本轮不推送", err)
		return
	}
	if len(s.Groups) == 0 && len(s.Privates) == 0 {
		log.Printf("settings.yaml 中 groups/privates 都为空，本轮不推送")
		return
	}
	for _, gid := range s.Groups {
		napcatPost(fmt.Sprintf("群 %d", gid), "/send_group_msg",
			map[string]any{"group_id": gid, "message": text})
	}
	for _, uid := range s.Privates {
		napcatPost(fmt.Sprintf("私聊 %d", uid), "/send_private_msg",
			map[string]any{"user_id": uid, "message": text})
	}
}

// napcatPost 向 NapCat 发起一次 action 请求；成功失败都只走日志，不向上返回 error。
func napcatPost(target, path string, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("推送 %s 失败（编码 payload）：%v", target, err)
		return
	}
	req, err := http.NewRequest(http.MethodPost, napcatBase+path, bytes.NewReader(body))
	if err != nil {
		log.Printf("推送 %s 失败（构造请求）：%v", target, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("推送 %s 失败（发送请求）：%v", target, err)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("推送 %s 失败（status %d）：%s",
			target, resp.StatusCode, truncate(string(respBody), 200))
		return
	}
	log.Printf("推送 %s 成功", target)
}

// ---------- settings.yaml ----------

// settings 是 settings.yaml 的结构；两个字段都允许缺省。
type settings struct {
	Groups   []int64 `yaml:"groups"`
	Privates []int64 `yaml:"privates"`
}

// loadSettings 每次发送前都重读 settings.yaml，使运行中修改即时生效。
//
// 候选路径按优先级：
//  1. 可执行文件同级目录（go build 后部署最常见）；
//  2. 当前工作目录（直接 ./rocom-merchant 启动时）；
//  3. 仓库内 cmd/rocom-merchant/（go run ./cmd/rocom-merchant 时友好）。
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
	return settings{}, fmt.Errorf("settings.yaml not found: %v", lastErr)
}

func candidateSettingsPaths() []string {
	var paths []string
	if exe, err := os.Executable(); err == nil {
		paths = append(paths, filepath.Join(filepath.Dir(exe), settingsName))
	}
	paths = append(paths, settingsName)
	paths = append(paths, filepath.Join("cmd", "rocom-merchant", settingsName))
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
