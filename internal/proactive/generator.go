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

// generatorSystemPrompt 是主动生成模型的硬约束。
//
// 主动消息不能暴露候选策略、好感度、白名单等内部依据，也不能把群聊/私聊历史串场；
// 这些规则放在系统消息里，避免每次组装提示词时散落重复文本。
const generatorSystemPrompt = `你负责为聊天机器人写一条简短、自然的主动开场白。

硬性规则：
- 只输出纯文本，不要 Markdown、列表、解释或引号。
- 不要使用 @，不要点名群成员。
- 不要提到好感度、分数、候选、策略、调度、Redis、白名单、内部配置等实现细节。
- 不要引用、复述或总结历史记录；历史只用于把握语气。
- 不要把私聊内容带到群聊，也不要把群聊内容带到私聊。
- 群聊里面向整个群自然开口；私聊里像熟人一样轻轻开启话题。
- 保持很短，中文不超过 60 字，英文不超过 25 个词。`

// forbiddenGeneratorFragments 是生成后的轻量防漏网。
//
// 提示词已经要求不要暴露内部词，但模型仍可能复述“策略/调度/Redis”等词；
// 发送前再做一次字符串拦截，失败交给调度器记录并等待下一轮。
var forbiddenGeneratorFragments = []string{
	"@", "```",
	"affinity", "score", "scoring", "strategy", "scheduler", "candidate", "redis", "internal", "whitelist",
	"好感", "分数", "策略", "候选", "调度", "白名单", "内部配置", "运行时开关",
	"私聊记录", "群聊记录", "聊天记录", "历史记录", "private history", "group history",
}

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
}

// Generator 负责生成短主动消息。
//
// 它只读取同一会话历史作为语气参考，不写入长期历史；真正发送和状态写入由
// Scheduler 完成。
type Generator struct {
	model   model.BaseChatModel
	history store.HistoryRepo
	log     *slog.Logger
	cfg     GeneratorConfig
}

// NewGenerator 构造主动消息生成器。
func NewGenerator(opts GeneratorOptions) *Generator {
	return &Generator{
		model:   opts.Model,
		history: opts.History,
		log:     cmp.Or(opts.Logger, slog.Default()),
		cfg:     opts.Config.withDefaults(),
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

	history, err := g.loadHistory(ctx, cand.SessionID)
	if err != nil {
		return "", err
	}
	messages := []*schema.Message{
		schema.SystemMessage(generatorSystemPrompt),
		schema.UserMessage(buildGeneratorPrompt(cand, now, history, g.cfg.MaxHistoryChars)),
	}
	reply, err := g.model.Generate(ctx, messages)
	if err != nil {
		return "", fmt.Errorf("proactive: generate message: %w", err)
	}
	text, err := cleanGeneratedText(reply.Content)
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
func buildGeneratorPrompt(cand Candidate, now time.Time, history []*schema.Message, maxHistoryChars int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "当前时间：%s\n", now.Format(time.RFC3339))
	fmt.Fprintf(&b, "会话类型：%s\n", humanConversationType(cand.ConvType))
	if cand.ConvType == domain.ConversationPrivate && cand.UserName != "" {
		fmt.Fprintf(&b, "私聊对象昵称：%s\n", cand.UserName)
	}
	if !cand.LastInboundAt.IsZero() {
		fmt.Fprintf(&b, "对方上次主动说话时间：%s\n", cand.LastInboundAt.Format(time.RFC3339))
	}
	if !cand.EventAt.IsZero() {
		fmt.Fprintf(&b, "最近群内活动时间：%s\n", cand.EventAt.Format(time.RFC3339))
	}
	b.WriteString("\n同一会话的最近历史（只用于语气，不要引用或复述）：\n")
	b.WriteString(formatHistory(history, maxHistoryChars))
	b.WriteString("\n请写一条可以直接发送的短消息。")
	return b.String()
}

// formatHistory 把历史压成短文本；超过 maxChars 时按 UTF-8 字符边界截断。
func formatHistory(history []*schema.Message, maxChars int) string {
	if len(history) == 0 {
		return "（无）\n"
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
		line := fmt.Sprintf("%s: %s\n", msg.Role, content)
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
		return "（无）\n"
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

// humanConversationType 把会话类型翻成中文，减少模型理解内部枚举的成本。
func humanConversationType(convType domain.ConversationType) string {
	switch convType {
	case domain.ConversationGroup:
		return "群聊"
	case domain.ConversationPrivate:
		return "私聊"
	default:
		return string(convType)
	}
}

// cleanGeneratedText 清理模型输出并拦截不应发送的片段。
//
// 它只做轻量规则：去掉外层引号、压平空白、检查禁用词；复杂安全判断仍由上游
// guard 和模型提示词负责。
func cleanGeneratedText(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	if strings.Contains(text, "```") {
		return "", fmt.Errorf("proactive: generated message contains forbidden fragment %q", "```")
	}
	text = strings.Trim(text, "\"'`“”‘’")
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return "", fmt.Errorf("proactive: generated empty message")
	}
	lower := strings.ToLower(text)
	for _, fragment := range forbiddenGeneratorFragments {
		if strings.Contains(lower, strings.ToLower(fragment)) {
			return "", fmt.Errorf("proactive: generated message contains forbidden fragment %q", fragment)
		}
	}
	return text, nil
}
