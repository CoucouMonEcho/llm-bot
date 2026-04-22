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
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是整个应用的配置根对象，字段与 configs/config.yaml 一一对应。
type Config struct {
	Server  Server  `yaml:"server"`
	Redis   Redis   `yaml:"redis"`
	LLM     LLM     `yaml:"llm"`
	Judge   LLM     `yaml:"judge"`
	Agent   Agent   `yaml:"agent"`
	Guard   Guard   `yaml:"guard"`
	Trigger Trigger `yaml:"trigger"`
	Log     Log     `yaml:"log"`
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

// Redis 只用于 History 的持久化；本项目不做集群/哨兵支持。
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
	return time.Duration(l.TimeoutSec) * time.Second
}

// Agent 聚合 Agent 层相关参数。
type Agent struct {
	// HistorySize 是 Redis 中每个 session 保留的最近对话条数上限。
	// 每次写入后通过 LTRIM 0 HistorySize-1 裁剪。
	HistorySize int `yaml:"history_size"`
	// PromptFile 是人设 YAML 文件的路径（相对工作目录）。
	PromptFile string `yaml:"prompt_file"`
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

	var cfg Config
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
	if len(c.Guard.FallbackReplies) == 0 {
		return fmt.Errorf("config: guard.fallback_replies must contain at least one entry")
	}
	return nil
}
