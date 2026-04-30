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
	Server        Server        `yaml:"server"`
	Redis         Redis         `yaml:"redis"`
	LLM           LLM           `yaml:"llm"`
	Judge         LLM           `yaml:"judge"`
	Agent         Agent         `yaml:"agent"`
	Guard         Guard         `yaml:"guard"`
	Trigger       Trigger       `yaml:"trigger"`
	GroupBuffer   GroupBuffer   `yaml:"group_buffer"`
	Blacklist     Blacklist     `yaml:"blacklist"`
	Log           Log           `yaml:"log"`
	Stats         Stats         `yaml:"stats"`
	Memory        Memory        `yaml:"memory"`
	PersonaTopics PersonaTopics `yaml:"persona_topics"`
	Proactive     Proactive     `yaml:"proactive"`
}

// Blacklist 控制需要在 Adapter 源头忽略的账号。
//
// UserIDs 使用字符串保存平台原始用户 ID，避免 QQ 号这类外部标识被当作数值参与
// 计算或格式化。命中后消息不会进入 bot 主循环、历史、stats 或 LLM 调用。
type Blacklist struct {
	UserIDs []string `yaml:"user_ids"`
}

// Stats 控制"人设参数"功能（当前含好感度 + 心情，可扩展为疲劳度、信任度等
// 影响回复风格的参数）。
//
// Enabled=false 时，bot 不调打分模型、agent 不读 Redis stats key，系统提示词
// 中也不会出现状态行。Enabled=true 时复用 cfg.Judge 的 LLM 作为打分
// 模型，不额外引入新的 LLM 配置段。
type Stats struct {
	Enabled bool `yaml:"enabled"`
	// ScorePromptFile 是 stats 打分模型 system prompt 的 YAML 文件路径。
	ScorePromptFile string `yaml:"score_prompt_file"`
}

// Memory 控制长期用户记忆功能。
//
// Enabled=false 时，agent 不读写用户记忆，系统提示词里也不会出现长期记忆块。
// Enabled=true 时复用 cfg.Judge 的 LLM 做回复后的异步摘要更新；Redis 中只保存
// 一段按"平台 + 用户"维度压缩后的事实文本。
type Memory struct {
	Enabled bool `yaml:"enabled"`
	// UpdatePromptFile 是长期记忆更新模型 system prompt 的 YAML 文件路径。
	UpdatePromptFile string `yaml:"update_prompt_file"`
	// MaxChars 限制注入和保存的长期记忆最大字符数。
	MaxChars int `yaml:"max_chars"`
}

// PersonaTopics 控制全局闲聊话题锚点。
//
// Enabled=false 时，agent 不读写话题锚点，系统提示词也不会出现话题块。
// Enabled=true 时复用 cfg.Judge 的 LLM 在正常回复后异步整理少量短话题；
// Redis 中使用一个全局 ZSET 保存 topic -> last_update_unix。
type PersonaTopics struct {
	Enabled bool `yaml:"enabled"`
	// MaxItems 限制注入和保存的最大话题数量。
	MaxItems int `yaml:"max_items"`
	// MaxAgeHours 限制话题最长存活时间，过期成员由 ZREMRANGEBYSCORE 清理。
	MaxAgeHours int `yaml:"max_age_hours"`
	// UpdatePromptFile 是话题更新模型 system prompt 的 YAML 文件路径。
	UpdatePromptFile string `yaml:"update_prompt_file"`
}

// MaxAge 把 MaxAgeHours 暴露成 time.Duration。
func (p PersonaTopics) MaxAge() time.Duration {
	if p.MaxAgeHours <= 0 {
		return 0
	}
	return time.Duration(p.MaxAgeHours) * time.Hour
}

// Proactive 控制主动发消息的静态策略。YAML 只保存部署期参数；
// 运行期开关保存在 Redis（key `bot_proactive_enabled`），避免改配置文件
// 才能临时停用主动发送。
//
// 决策面只剩"群冷却 + 时间窗 + Redis 开关"三件事；好感度排行 / 白名单 /
// 日限额 / 会话冷却 / 用户活跃时间窗等参数都已删除：群冷却阈值本身就是
// 频率约束，"群里 1h 没人说话才主动开口"在直觉上也容易解释。
//
// 这里的时间字段保留为秒数，是为了让配置文件易读；对外统一通过方法转成
// time.Duration，避免调用方散落重复换算。
type Proactive struct {
	// PromptFile 是主动消息生成提示词 YAML 文件路径。
	PromptFile string `yaml:"prompt_file"`
	// WindowStart / WindowEnd 是每天允许主动发送的时间窗边界，格式为 HH:MM。
	// 默认允许跨天窗口（例如 10:00 到 01:00），以覆盖深夜仍活跃的群。
	WindowStart string `yaml:"window_start"`
	WindowEnd   string `yaml:"window_end"`
	// IntervalSec 是调度器基础扫描间隔；真实间隔还会叠加 JitterMaxSec 抖动。
	IntervalSec int `yaml:"interval_sec"`
	// JitterMaxSec 是每轮调度额外随机等待的上限，用来打散固定整点发送痕迹。
	JitterMaxSec int `yaml:"jitter_max_sec"`
	// IdleThresholdSec 是"群里多久没人说话才主动开口"的阈值。
	IdleThresholdSec int `yaml:"idle_threshold_sec"`
}

// Interval 返回主动调度的基础间隔。
func (p Proactive) Interval() time.Duration {
	return seconds(p.IntervalSec)
}

// JitterMax 返回主动调度额外抖动的上限。
func (p Proactive) JitterMax() time.Duration {
	return seconds(p.JitterMaxSec)
}

// IdleThreshold 返回群冷却阈值。
func (p Proactive) IdleThreshold() time.Duration {
	return seconds(p.IdleThresholdSec)
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

// Guard 描述固定启用的 LLM 裁判参数；裁判未明确输出 safe 时静默不回复。
type Guard struct {
	// JudgePromptFile 是 LLM 裁判 system prompt 的 YAML 文件路径。
	// 裁判固定作为输入侧安全判定：只有明确输出 safe 才进入主链。
	JudgePromptFile string `yaml:"judge_prompt_file"`
}

// Trigger 控制哪些消息会被 Adapter 投递给下游。
type Trigger struct {
	// Private 为 true 时，所有私聊消息都直接触发。
	Private bool `yaml:"private"`
	// Prefix 是显式命令前缀，命中时即便不 @ 也会触发。
	Prefix []string `yaml:"prefix"`
	// GroupFollowupSec 控制群聊普通消息跟进窗口；<=0 表示关闭。
	GroupFollowupSec int `yaml:"group_followup_sec"`
}

// GroupBuffer 控制"群聊短期上下文缓存"。
//
// 群里非显式触发的普通消息既不进对话历史、也不入个人长期记忆，但 @bot
// 时如果完全无视前几句聊天，回复会很突兀。本段只配置容量与生命周期，
// 真正的写读由 store.GroupBufferRepo 完成。
//
// Enabled=false 时上层 Adapter 直接跳过写入，store 也不会被构造，相当于
// "回复纯靠当前消息，不参考刚才在聊什么"。
type GroupBuffer struct {
	// Enabled 是否启用群聊短期上下文。默认 true。
	Enabled bool `yaml:"enabled"`
	// MaxMessages 单个群在 Redis List 里保留的最大消息数；
	// 写入侧靠 LTRIM 自动裁剪。<=0 时回退默认 20。
	MaxMessages int `yaml:"max_messages"`
	// TTLSec 每次写入后刷新的滑动过期秒数；<=0 时回退默认 600（10 分钟）。
	// 短窗口足够覆盖"刚才大家在聊什么"，又能让冷清群里的旧上下文自然消失。
	TTLSec int `yaml:"ttl_sec"`
}

// TTL 把 TTLSec 暴露成 time.Duration，避免调用方散落 *time.Second 换算。
func (g GroupBuffer) TTL() time.Duration { return seconds(g.TTLSec) }

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
		Proactive:     defaultProactive(),
		Memory:        defaultMemory(),
		PersonaTopics: defaultPersonaTopics(),
		GroupBuffer:   defaultGroupBuffer(),
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
	if c.Judge.BaseURL == "" || c.Judge.Model == "" {
		return fmt.Errorf("config: judge.base_url and judge.model are required")
	}
	c.Guard.JudgePromptFile = strings.TrimSpace(c.Guard.JudgePromptFile)
	if c.Guard.JudgePromptFile == "" {
		return fmt.Errorf("config: guard.judge_prompt_file is required")
	}
	c.Stats.ScorePromptFile = strings.TrimSpace(c.Stats.ScorePromptFile)
	if c.Stats.Enabled && c.Stats.ScorePromptFile == "" {
		return fmt.Errorf("config: stats.score_prompt_file is required when stats.enabled")
	}
	c.Memory.UpdatePromptFile = strings.TrimSpace(c.Memory.UpdatePromptFile)
	if c.Memory.Enabled && c.Memory.UpdatePromptFile == "" {
		return fmt.Errorf("config: memory.update_prompt_file is required when memory.enabled")
	}
	if c.Memory.MaxChars <= 0 {
		c.Memory.MaxChars = defaultMemory().MaxChars
	}
	c.normalizePersonaTopics()
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
	c.normalizeBlacklist()
	c.normalizeGroupBuffer()
	if err := c.validateProactive(); err != nil {
		return err
	}
	return nil
}

// normalizeGroupBuffer 给"群聊短期上下文"做轻量参数兜底。
//
// 故意不返回 error：MaxMessages / TTLSec 配错只意味着上下文窗口不理想，
// 让进程崩在这种纯调参问题上得不偿失，直接拉回默认值即可。
func (c *Config) normalizeGroupBuffer() {
	defaults := defaultGroupBuffer()
	if c.GroupBuffer.MaxMessages <= 0 {
		c.GroupBuffer.MaxMessages = defaults.MaxMessages
	}
	if c.GroupBuffer.TTLSec <= 0 {
		c.GroupBuffer.TTLSec = defaults.TTLSec
	}
}

func (c *Config) normalizeBlacklist() {
	seen := make(map[string]struct{}, len(c.Blacklist.UserIDs))
	userIDs := c.Blacklist.UserIDs[:0]
	for _, userID := range c.Blacklist.UserIDs {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		userIDs = append(userIDs, userID)
	}
	c.Blacklist.UserIDs = userIDs
}

func defaultProactive() Proactive {
	return Proactive{
		PromptFile:       "configs/prompts/persona.yaml",
		WindowStart:      "10:00",
		WindowEnd:        "01:00",
		IntervalSec:      int((10 * time.Minute) / time.Second),
		JitterMaxSec:     int((1 * time.Minute) / time.Second),
		IdleThresholdSec: int((1 * time.Hour) / time.Second),
	}
}

func defaultMemory() Memory {
	return Memory{
		Enabled:          false,
		UpdatePromptFile: "configs/prompts/memory_update.yaml",
		MaxChars:         1200,
	}
}

func defaultPersonaTopics() PersonaTopics {
	return PersonaTopics{
		Enabled:          true,
		MaxItems:         5,
		MaxAgeHours:      12,
		UpdatePromptFile: "configs/prompts/persona_topics_update.yaml",
	}
}

func (c *Config) normalizePersonaTopics() {
	defaults := defaultPersonaTopics()
	if c.PersonaTopics.MaxItems <= 0 {
		c.PersonaTopics.MaxItems = defaults.MaxItems
	}
	if c.PersonaTopics.MaxAgeHours <= 0 {
		c.PersonaTopics.MaxAgeHours = defaults.MaxAgeHours
	}
	c.PersonaTopics.UpdatePromptFile = strings.TrimSpace(c.PersonaTopics.UpdatePromptFile)
	if c.PersonaTopics.UpdatePromptFile == "" {
		c.PersonaTopics.UpdatePromptFile = defaults.UpdatePromptFile
	}
}

// defaultGroupBuffer 给"群聊短期上下文"一份保守可用的默认参数：
// 启用、保留 20 条、滑动 10 分钟过期。容量与 TTL 都是"看上去够用"的
// 直觉值，调错了顶多上下文丢一两条，不需要崩进程。
func defaultGroupBuffer() GroupBuffer {
	return GroupBuffer{
		Enabled:     true,
		MaxMessages: 20,
		TTLSec:      int((10 * time.Minute) / time.Second),
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
	if c.Proactive.IntervalSec <= 0 {
		c.Proactive.IntervalSec = defaults.IntervalSec
	}
	if c.Proactive.JitterMaxSec < 0 {
		return fmt.Errorf("config: proactive.jitter_max_sec must be >= 0")
	}
	if c.Proactive.IdleThresholdSec <= 0 {
		c.Proactive.IdleThresholdSec = defaults.IdleThresholdSec
	}

	if _, err := time.Parse("15:04", c.Proactive.WindowStart); err != nil {
		return fmt.Errorf("config: proactive.window_start must use HH:MM: %w", err)
	}
	if _, err := time.Parse("15:04", c.Proactive.WindowEnd); err != nil {
		return fmt.Errorf("config: proactive.window_end must use HH:MM: %w", err)
	}
	return nil
}
