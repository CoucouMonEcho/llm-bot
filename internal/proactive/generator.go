// Package proactive 的 generator.go 实现主动开场白生成。
//
// 生成器只读同一会话历史作为语气参考，不写历史、不改状态；调度器负责发送后的
// 冷却、日限额和 PendingContext 写入。
package proactive

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/domain"
	"github.com/echo/llm-bot/internal/store"
)

// GeneratorConfig 控制主动开场白生成时可读取的上下文量。
//
// 历史只用于模仿当前会话语气，不允许模型引用或总结；因此默认读取很少几条。
type GeneratorConfig struct {
	// HistorySize 表示传给模型的同会话历史条数，只用于语气参考。
	// 0 使用默认值，负数表示不读取历史。
	HistorySize int
	// MaxHistoryChars 限制写入提示词的历史文本长度。
	MaxHistoryChars int
}

// DefaultGeneratorConfig 返回较保守的生成默认值。
//
// 历史条数和字符数都偏小，优先降低串场和泄露上下文的风险。
func DefaultGeneratorConfig() GeneratorConfig {
	return GeneratorConfig{
		HistorySize:     6,
		MaxHistoryChars: 1200,
	}
}

// withDefaults 补齐生成配置；HistorySize 负数表示显式关闭历史读取。
func (c GeneratorConfig) withDefaults() GeneratorConfig {
	d := DefaultGeneratorConfig()
	if c.HistorySize == 0 {
		c.HistorySize = d.HistorySize
	}
	if c.MaxHistoryChars <= 0 {
		c.MaxHistoryChars = d.MaxHistoryChars
	}
	return c
}

// GeneratorOptions 汇总 Generator 的模型、历史仓库和配置。
//
// History 可以为 nil，此时生成器只根据候选元信息写开场白。
type GeneratorOptions struct {
	Model   model.BaseChatModel
	History store.HistoryRepo
	Logger  *slog.Logger
	Config  GeneratorConfig
	Prompts GeneratorPrompts
}

// Generator 负责生成短主动消息。
//
// 它只读取同一会话历史作为语气参考，不写入长期历史；真正发送和状态写入由
// Scheduler 完成。
type Generator struct {
	model      model.BaseChatModel
	history    store.HistoryRepo
	log        *slog.Logger
	cfg        GeneratorConfig
	prompts    GeneratorPrompts
	promptsErr error
}

// NewGenerator 构造主动消息生成器。
func NewGenerator(opts GeneratorOptions) *Generator {
	prompts, promptsErr := opts.Prompts.normalized()
	return &Generator{
		model:      opts.Model,
		history:    opts.History,
		log:        cmp.Or(opts.Logger, slog.Default()),
		cfg:        opts.Config.withDefaults(),
		prompts:    prompts,
		promptsErr: promptsErr,
	}
}

// Generate 为候选目标生成一条可直接发送的主动消息。
//
// 生成结果会经过 cleanGeneratedText 清理和敏感片段检查；不合格时返回错误，
// 调度器会放弃本轮发送。
func (g *Generator) Generate(ctx context.Context, cand Candidate, now time.Time) (string, error) {
	if g == nil || g.model == nil {
		return "", fmt.Errorf("proactive: nil generator model")
	}
	if cand.Platform == "" || cand.ConvType == "" || cand.SessionID == "" {
		return "", fmt.Errorf("proactive: incomplete candidate")
	}
	if g.promptsErr != nil {
		return "", fmt.Errorf("proactive: generator prompts: %w", g.promptsErr)
	}

	history, err := g.loadHistory(ctx, cand.SessionID)
	if err != nil {
		return "", err
	}
	messages := []*schema.Message{
		schema.SystemMessage(g.prompts.System),
		schema.UserMessage(buildGeneratorPrompt(g.prompts, cand, now, history, g.cfg.MaxHistoryChars)),
	}
	reply, err := g.model.Generate(ctx, messages)
	if err != nil {
		return "", fmt.Errorf("proactive: generate message: %w", err)
	}
	text, err := cleanGeneratedText(reply.Content, g.prompts.ForbiddenFragments)
	if err != nil {
		return "", err
	}
	return text, nil
}

// loadHistory 读取同会话最近历史；HistorySize < 0 时显式跳过。
func (g *Generator) loadHistory(ctx context.Context, sessionID string) ([]*schema.Message, error) {
	if g.history == nil || g.cfg.HistorySize < 0 {
		return nil, nil
	}
	msgs, err := g.history.Load(ctx, sessionID, g.cfg.HistorySize)
	if err != nil {
		return nil, fmt.Errorf("proactive: load history: %w", err)
	}
	return msgs, nil
}

// buildGeneratorPrompt 组装用户消息，把候选来源转成模型能理解的上下文。
//
// 这里不写入 Source/Affinity 等内部字段，只给时间、会话类型和同会话历史。
func buildGeneratorPrompt(prompts GeneratorPrompts, cand Candidate, now time.Time, history []*schema.Message, maxHistoryChars int) string {
	var b strings.Builder
	up := prompts.UserPrompt
	fmt.Fprintf(&b, "%s%s\n", up.CurrentTimeLabel, now.Format(time.RFC3339))
	fmt.Fprintf(&b, "%s%s\n", up.ConversationTypeLabel, humanConversationType(up.ConversationTypes, cand.ConvType))
	if cand.ConvType == domain.ConversationPrivate && cand.UserName != "" {
		fmt.Fprintf(&b, "%s%s\n", up.PrivateDisplayNameLabel, cand.UserName)
	}
	if !cand.LastInboundAt.IsZero() {
		fmt.Fprintf(&b, "%s%s\n", up.LastInboundAtLabel, cand.LastInboundAt.Format(time.RFC3339))
	}
	if !cand.EventAt.IsZero() {
		fmt.Fprintf(&b, "%s%s\n", up.RecentGroupActivityLabel, cand.EventAt.Format(time.RFC3339))
	}
	b.WriteByte('\n')
	b.WriteString(up.HistoryHeader)
	b.WriteByte('\n')
	b.WriteString(formatHistory(history, maxHistoryChars, up.NoHistoryText))
	b.WriteByte('\n')
	b.WriteString(up.Closing)
	return b.String()
}

// formatHistory 把历史压成短文本；超过 maxChars 时按 UTF-8 字符边界截断。
func formatHistory(history []*schema.Message, maxChars int, noHistoryText string) string {
	if len(history) == 0 {
		return noHistoryText + "\n"
	}
	var b strings.Builder
	for _, msg := range history {
		if msg == nil {
			continue
		}
		content := strings.TrimSpace(msg.Content)
		if content == "" {
			continue
		}
		role := string(msg.Role)
		if msg.Name != "" {
			role = fmt.Sprintf("%s(%s)", role, msg.Name)
		}
		line := fmt.Sprintf("%s: %s\n", role, content)
		if maxChars > 0 && b.Len()+len(line) > maxChars {
			remaining := maxChars - b.Len()
			if remaining > 0 {
				b.WriteString(truncateString(line, remaining))
			}
			break
		}
		b.WriteString(line)
	}
	if b.Len() == 0 {
		return noHistoryText + "\n"
	}
	return b.String()
}

// truncateString 按字节上限截断，但不会切断 UTF-8 字符。
func truncateString(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	last := 0
	for i := range s {
		if i > maxBytes {
			return s[:last]
		}
		last = i
	}
	return s
}

// humanConversationType 把会话类型翻成配置文案，减少模型理解内部枚举的成本。
func humanConversationType(labels map[string]string, convType domain.ConversationType) string {
	if label := labels[string(convType)]; label != "" {
		return label
	}
	return string(convType)
}

// cleanGeneratedText 清理模型输出并拦截不应发送的片段。
//
// 它只做轻量规则：去掉外层引号、压平空白、检查禁用词；复杂安全判断仍由上游
// guard 和模型提示词负责。
func cleanGeneratedText(raw string, forbidden []string) (string, error) {
	text := strings.TrimSpace(raw)
	if fragment, ok := containsForbiddenFragment(text, forbidden); ok {
		return "", fmt.Errorf("proactive: generated message contains forbidden fragment %q", fragment)
	}
	text = strings.Trim(text, "\"'`“”‘’")
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return "", fmt.Errorf("proactive: generated empty message")
	}
	if fragment, ok := containsForbiddenFragment(text, forbidden); ok {
		return "", fmt.Errorf("proactive: generated message contains forbidden fragment %q", fragment)
	}
	return text, nil
}

func containsForbiddenFragment(text string, forbidden []string) (string, bool) {
	lower := strings.ToLower(text)
	for _, fragment := range forbidden {
		fragment = strings.TrimSpace(fragment)
		if fragment != "" && strings.Contains(lower, strings.ToLower(fragment)) {
			return fragment, true
		}
	}
	return "", false
}
