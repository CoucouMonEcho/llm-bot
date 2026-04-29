// Package config 负责加载并校验应用配置。
//
// 设计原则：
//  1. 单一入口：整个项目仅 Load() 一个函数负责把 YAML 文件 + 环境变量解析
//     为强类型的 *Config，其他模块只能只读地使用返回值。
//  2. 环境变量仅覆盖"敏感 / 部署相关"字段：API key、Redis 密码、access_token
//     等——这是容器部署时注入的典型需求。其余字段一律来自 YAML，避免
//     "全字段 env 覆盖"的反射魔法换来一堆不会被用到的灵活性。
//  3. 失败即崩溃：配置错误属于启动期问题，没必要兜底，直接返回 error 让
//     main 打印日志后退出。
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是整个应用的配置根对象，字段与 configs/config.yaml 一一对应。
type Config struct {
	Server    Server    `yaml:"server"`
	Redis     Redis     `yaml:"redis"`
	LLM       LLM       `yaml:"llm"`
	Judge     LLM       `yaml:"judge"`
	Agent     Agent     `yaml:"agent"`
	Guard     Guard     `yaml:"guard"`
	Trigger   Trigger   `yaml:"trigger"`
	Log       Log       `yaml:"log"`
	Stats     Stats     `yaml:"stats"`
	Proactive Proactive `yaml:"proactive"`
}

// Stats 控制"人设参数"功能（当前含好感度 + 心情，可扩展为疲劳度、信任度等
// 影响回复风格的参数）。
//
// Enabled=false 时，bot 不调打分模型、agent 不读 Redis stats key，系统提示词
// 中也不会出现状态行。Enabled=true 时复用 cfg.Judge 的 LLM 作为打分
// 模型，不额外引入新的 LLM 配置段。
type Stats struct {
	Enabled bool `yaml:"enabled"`
}

// Proactive 控制主动发消息的静态策略。YAML 只保存部署期参数；
// 运行期开关和群白名单保存在 Redis，避免改配置文件才能临时停用某个群。
//
// 只有 Enabled=true 且 Redis 开关也开启时，scheduler 才会尝试发送。
// 这里的时间字段保留为秒数，是为了让配置文件易读；对外统一通过方法转成
// time.Duration，避免调用方散落重复换算。
type Proactive struct {
	// Enabled 是配置侧总开关；关闭时不构造主动消息调度链路。
	Enabled bool `yaml:"enabled"`
	// PromptFile 是主动消息生成提示词 YAML 文件路径；仅在 Enabled=true 时读取。
	PromptFile string `yaml:"prompt_file"`
	// WindowStart / WindowEnd 是每天允许主动发送的时间窗边界，格式为 HH:MM。
	// 默认允许跨天窗口（例如 10:00 到 01:00），以覆盖深夜仍活跃的群。
	WindowStart string `yaml:"window_start"`
	WindowEnd   string `yaml:"window_end"`
	// MinSinceLastInboundSec 要求用户最后发言至少过去多久，避免刚聊完就追发。
	MinSinceLastInboundSec int `yaml:"min_since_last_inbound_sec"`
	// MaxSinceLastInboundSec 要求用户最后发言不能太久远，避免打扰已经沉寂的会话。
	MaxSinceLastInboundSec int `yaml:"max_since_last_inbound_sec"`
	// IntervalSec 是调度器基础扫描间隔；真实间隔还会叠加 JitterMaxSec 抖动。
	IntervalSec int `yaml:"interval_sec"`
	// JitterMaxSec 是每轮调度额外随机等待的上限，用来打散固定整点发送痕迹。
	JitterMaxSec int `yaml:"jitter_max_sec"`
	// TopN 限制每轮只从最近最相关的一批候选中挑选，避免全量扫描 Redis 排行。
	TopN int `yaml:"top_n"`
	// DailyLimit 限制单日主动发送总量，防止异常配置或模型输出造成刷屏。
	DailyLimit int `yaml:"daily_limit"`
	// SessionCooldownSec 限制同一会话的主动发送频率。
	SessionCooldownSec int `yaml:"session_cooldown_sec"`
	// PendingTTLSec 是主动消息短上下文的保留时长，用于接住用户对主动消息的回复。
	PendingTTLSec int `yaml:"pending_ttl_sec"`
	// RecentEventsCap 限制每个会话记录的近期事件数量，控制候选选择时的 Redis 成本。
	RecentEventsCap int `yaml:"recent_events_cap"`
	// DryRun 只生成和记录决策，不真正发送；用于上线前观察候选质量。
	DryRun bool `yaml:"dry_run"`
}

// MinSinceLastInbound 返回用户最后发言的最短间隔。
func (p Proactive) MinSinceLastInbound() time.Duration {
	return seconds(p.MinSinceLastInboundSec)
}

// MaxSinceLastInbound 返回用户最后发言的最长间隔。
func (p Proactive) MaxSinceLastInbound() time.Duration {
	return seconds(p.MaxSinceLastInboundSec)
}

// Interval 返回主动调度的基础间隔。
func (p Proactive) Interval() time.Duration {
	return seconds(p.IntervalSec)
}

// JitterMax 返回主动调度额外抖动的上限。
func (p Proactive) JitterMax() time.Duration {
	return seconds(p.JitterMaxSec)
}

// SessionCooldown 返回同一 session 的主动发送冷却时间。
func (p Proactive) SessionCooldown() time.Duration {
	return seconds(p.SessionCooldownSec)
}

// PendingTTL 返回主动消息短上下文的保留时长。
func (p Proactive) PendingTTL() time.Duration {
	return seconds(p.PendingTTLSec)
}

// Server 描述对外提供的 HTTP / WebSocket 服务。
type Server struct {
	// Addr 是 net.Listen 兼容的地址字符串，例如 ":8080" 或 "0.0.0.0:8080"。
	Addr string `yaml:"addr"`
	// WSPath 是反向 WS 的挂载路径，NapCatQQ 必须连接到这个路径。
	WSPath string `yaml:"ws_path"`
	// AccessToken 用于校验 NapCat 在握手时传入的凭证。空字符串表示不校验。
	AccessToken string `yaml:"access_token"`
}

// Redis 用于 History、stats 和 proactive 运行期状态；本项目不做集群/哨兵支持。
type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// LLM 描述一个 OpenAI 兼容的对话模型接入参数。
// 同时用于主模型和裁判模型（Config.LLM / Config.Judge）。
type LLM struct {
	BaseURL    string `yaml:"base_url"`
	APIKey     string `yaml:"api_key"`
	Model      string `yaml:"model"`
	TimeoutSec int    `yaml:"timeout_sec"`
}

// Timeout 便捷方法，将秒数转为 time.Duration；0 表示不超时。
func (l LLM) Timeout() time.Duration {
	if l.TimeoutSec <= 0 {
		return 0
	}
	return seconds(l.TimeoutSec)
}

func seconds(v int) time.Duration {
	if v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Second
}

// Agent 聚合 Agent 层相关参数。
type Agent struct {
	// HistorySize 是 Redis 中每个 session 保留的最近对话条数上限。
	// 每次写入后通过 LTRIM 0 HistorySize-1 裁剪。
	HistorySize int `yaml:"history_size"`
	// PromptFile 是人设 YAML 文件的路径（相对工作目录）。
	PromptFile string `yaml:"prompt_file"`
	// EmptyReplyFallback 是主链回复清洗后为空时发送的兜底文案。
	EmptyReplyFallback string `yaml:"empty_reply_fallback"`
}

// Guard 描述提示词注入防护相关参数。
type Guard struct {
	// RegexPatterns 是第一级同步黑名单；任一命中即立即降级。
	RegexPatterns []string `yaml:"regex_patterns"`
	// JudgeEnabled 控制是否启用并行 LLM 裁判。关闭后仅保留正则 + prompt 包装两道防线。
	JudgeEnabled bool `yaml:"judge_enabled"`
	// FallbackReplies 是降级回复的候选池，运行时随机挑选一条。
	FallbackReplies []string `yaml:"fallback_replies"`
}

// Trigger 控制哪些消息会被 Adapter 投递给下游。
type Trigger struct {
	// Private 为 true 时，所有私聊消息都直接触发。
	Private bool `yaml:"private"`
	// GroupAtOnly 为 true 时，群聊消息必须 @ 机器人才触发。
	GroupAtOnly bool `yaml:"group_at_only"`
	// Prefix 是显式命令前缀，命中时即便不 @ 也会触发。
	Prefix []string `yaml:"prefix"`
}

// Log 日志级别配置。
type Log struct {
	// Level 取值：debug / info / warn / error。
	Level string `yaml:"level"`
}

// Load 从给定的配置文件路径加载配置，并对敏感字段应用环境变量覆盖。
//
// path 为 yaml 文件的路径，例如 "configs/config.yaml"。
//
// 支持的环境变量（均为可选；非空时覆盖 YAML 中对应字段）：
//
//	LLMBOT_LLM_API_KEY          -> llm.api_key
//	LLMBOT_JUDGE_API_KEY        -> judge.api_key
//	LLMBOT_REDIS_PASSWORD       -> redis.password
//	LLMBOT_SERVER_ACCESS_TOKEN  -> server.access_token
//
// 返回填充完整的 *Config，若解析或校验失败则返回 error。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	cfg := Config{
		Proactive: defaultProactive(),
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("config: unmarshal %s: %w", path, err)
	}

	cfg.applyEnv()

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applyEnv 把预定义的环境变量覆盖到配置里。
//
// 只处理"典型需要注入"的少量字段——刻意不做全字段反射覆盖，
// 以换取配置来源的可读性与可预测性：任何字段只会从 YAML 或这几个
// 显式列出的变量进入，不存在"改个环境变量就悄悄影响某个深层字段"的惊喜。
func (c *Config) applyEnv() {
	if v := os.Getenv("LLMBOT_LLM_API_KEY"); v != "" {
		c.LLM.APIKey = v
	}
	if v := os.Getenv("LLMBOT_JUDGE_API_KEY"); v != "" {
		c.Judge.APIKey = v
	}
	if v := os.Getenv("LLMBOT_REDIS_PASSWORD"); v != "" {
		c.Redis.Password = v
	}
	if v := os.Getenv("LLMBOT_SERVER_ACCESS_TOKEN"); v != "" {
		c.Server.AccessToken = v
	}
}

// validate 做最基本的字段必填校验，防止空值导致运行期报难定位的错误。
func (c *Config) validate() error {
	if c.Server.Addr == "" {
		return fmt.Errorf("config: server.addr is required")
	}
	if c.Server.WSPath == "" {
		return fmt.Errorf("config: server.ws_path is required")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("config: redis.addr is required")
	}
	if c.LLM.BaseURL == "" || c.LLM.Model == "" {
		return fmt.Errorf("config: llm.base_url and llm.model are required")
	}
	if c.Guard.JudgeEnabled && (c.Judge.BaseURL == "" || c.Judge.Model == "") {
		return fmt.Errorf("config: judge.base_url and judge.model are required when guard.judge_enabled")
	}
	if c.Agent.PromptFile == "" {
		return fmt.Errorf("config: agent.prompt_file is required")
	}
	if c.Agent.HistorySize <= 0 {
		c.Agent.HistorySize = 20
	}
	c.Agent.EmptyReplyFallback = strings.TrimSpace(c.Agent.EmptyReplyFallback)
	if c.Agent.EmptyReplyFallback == "" {
		c.Agent.EmptyReplyFallback = "我去洗澡了"
	}
	if len(c.Guard.FallbackReplies) == 0 {
		return fmt.Errorf("config: guard.fallback_replies must contain at least one entry")
	}
	if err := c.validateProactive(); err != nil {
		return err
	}
	return nil
}

func defaultProactive() Proactive {
	return Proactive{
		Enabled:                false,
		PromptFile:             "configs/prompts/proactive.yaml",
		WindowStart:            "10:00",
		WindowEnd:              "01:00",
		MinSinceLastInboundSec: int((1 * time.Hour) / time.Second),
		MaxSinceLastInboundSec: int((6 * time.Hour) / time.Second),
		IntervalSec:            int((1 * time.Hour) / time.Second),
		JitterMaxSec:           int((10 * time.Minute) / time.Second),
		TopN:                   50,
		DailyLimit:             3,
		SessionCooldownSec:     int((6 * time.Hour) / time.Second),
		PendingTTLSec:          int((30 * time.Minute) / time.Second),
		RecentEventsCap:        200,
		DryRun:                 true,
	}
}

func (c *Config) validateProactive() error {
	defaults := defaultProactive()
	c.Proactive.PromptFile = strings.TrimSpace(c.Proactive.PromptFile)
	if c.Proactive.PromptFile == "" {
		c.Proactive.PromptFile = defaults.PromptFile
	}
	if c.Proactive.WindowStart == "" {
		c.Proactive.WindowStart = defaults.WindowStart
	}
	if c.Proactive.WindowEnd == "" {
		c.Proactive.WindowEnd = defaults.WindowEnd
	}
	if c.Proactive.MinSinceLastInboundSec <= 0 {
		c.Proactive.MinSinceLastInboundSec = defaults.MinSinceLastInboundSec
	}
	if c.Proactive.MaxSinceLastInboundSec <= 0 {
		c.Proactive.MaxSinceLastInboundSec = defaults.MaxSinceLastInboundSec
	}
	if c.Proactive.IntervalSec <= 0 {
		c.Proactive.IntervalSec = defaults.IntervalSec
	}
	if c.Proactive.JitterMaxSec < 0 {
		return fmt.Errorf("config: proactive.jitter_max_sec must be >= 0")
	}
	if c.Proactive.TopN <= 0 {
		c.Proactive.TopN = defaults.TopN
	}
	if c.Proactive.DailyLimit <= 0 {
		c.Proactive.DailyLimit = defaults.DailyLimit
	}
	if c.Proactive.SessionCooldownSec <= 0 {
		c.Proactive.SessionCooldownSec = defaults.SessionCooldownSec
	}
	if c.Proactive.PendingTTLSec <= 0 {
		c.Proactive.PendingTTLSec = defaults.PendingTTLSec
	}
	if c.Proactive.RecentEventsCap <= 0 {
		c.Proactive.RecentEventsCap = defaults.RecentEventsCap
	}

	if _, err := time.Parse("15:04", c.Proactive.WindowStart); err != nil {
		return fmt.Errorf("config: proactive.window_start must use HH:MM: %w", err)
	}
	if _, err := time.Parse("15:04", c.Proactive.WindowEnd); err != nil {
		return fmt.Errorf("config: proactive.window_end must use HH:MM: %w", err)
	}
	if c.Proactive.MinSinceLastInboundSec > c.Proactive.MaxSinceLastInboundSec {
		return fmt.Errorf("config: proactive.min_since_last_inbound_sec must be <= proactive.max_since_last_inbound_sec")
	}
	return nil
}
