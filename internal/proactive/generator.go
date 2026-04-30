// Package proactive 的 generator.go 实现群主动开场白生成。
//
// 生成器只读同一群的对话历史作为语气参考，不写历史、不改状态；调度器负责
// 发送和发送后的状态回写。当前主动消息只面向群聊，不存在私聊语境。
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
	"github.com/echo/llm-bot/internal/store"
)

// GeneratorOptions 汇总 Generator 的模型、历史仓库和配置。
//
// History 可以为 nil，此时生成器只根据时间信息写开场白。Config 复用扁平的
// proactive.Config，Generator 自己只读其中两个字段。
type GeneratorOptions struct {
	Model   model.BaseChatModel
	History store.HistoryRepo
	Logger  *slog.Logger
	Config  Config
	Prompts GeneratorPrompts
}

// Generator 负责生成短主动消息。
//
// 它只读取同一群的历史作为语气参考，不写入长期历史；真正发送和状态写入由
// Scheduler 完成。历史只用于模仿当前会话语气，不允许模型引用或总结；因此
// 默认读取很少几条。historySize 为负数时显式跳过历史读取。
type Generator struct {
	model           model.BaseChatModel
	history         store.HistoryRepo
	log             *slog.Logger
	historySize     int
	maxHistoryChars int
	prompts         GeneratorPrompts
}

// NewGenerator 构造主动消息生成器。
//
// opts.Prompts 应该来自 LoadGeneratorPrompts，已经过 normalized 校验；
// 这里不再做二次兜底，避免装配期问题被静默压到运行期。
func NewGenerator(opts GeneratorOptions) *Generator {
	return &Generator{
		model:           opts.Model,
		history:         opts.History,
		log:             cmp.Or(opts.Logger, slog.Default()),
		historySize:     opts.Config.HistorySize,
		maxHistoryChars: opts.Config.MaxHistoryChars,
		prompts:         opts.Prompts,
	}
}

// Generate 为某个群生成一条可直接发送的主动开场白。
//
// 入参只保留三件事：群 sessionID、群里上一次互动时间、当前时间。生成结果
// 经过 cleanGeneratedText 清理和敏感片段检查；不合格时返回错误，调度器会
// 放弃本轮发送。历史已在 store.Load 出口处自带 [YYYY-MM-DD HH:MM] 前缀，
// formatHistory 不再额外渲染时间。
func (g *Generator) Generate(ctx context.Context, sessionID string, lastInboundAt, now time.Time) (string, error) {
	if g == nil || g.model == nil {
		return "", fmt.Errorf("proactive: nil generator model")
	}
	if sessionID == "" {
		return "", fmt.Errorf("proactive: empty session id")
	}

	history, err := g.loadHistory(ctx, sessionID)
	if err != nil {
		return "", err
	}
	messages := []*schema.Message{
		schema.SystemMessage(buildSystemPrompt(g.prompts)),
		schema.UserMessage(buildGeneratorPrompt(g.prompts, now, lastInboundAt, history, g.maxHistoryChars)),
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

// loadHistory 读取同会话最近历史；historySize < 0 时显式跳过。
func (g *Generator) loadHistory(ctx context.Context, sessionID string) ([]*schema.Message, error) {
	if g.history == nil || g.historySize < 0 {
		return nil, nil
	}
	msgs, err := g.history.Load(ctx, sessionID, g.historySize)
	if err != nil {
		return nil, fmt.Errorf("proactive: load history: %w", err)
	}
	return msgs, nil
}

// buildSystemPrompt 把规则 system prompt 与 examples 渲染成最终 system 文本。
//
// few-shot 这里走"结构化输出风格示例"路线，而不是伪造多轮 user/assistant 消息：
// 主动消息的真实输入并不是用户自然语言，而是"当前时间+历史摘要+生成任务"，
// 让配置作者去写 synthetic user 既容易写错，也容易让模型把它误当成真实上下文。
// 直接列出"输出应该长这样"的 assistant 正例，更贴合本场景，也更容易被模型模仿。
//
// examples 为空时直接返回原 system，不附加任何分隔，避免出现尾部空段。
func buildSystemPrompt(prompts GeneratorPrompts) string {
	if len(prompts.Examples) == 0 {
		return prompts.System
	}
	var b strings.Builder
	b.Grow(len(prompts.System) + 64 + len(prompts.Examples)*16)
	b.WriteString(prompts.System)
	b.WriteString("\n\n输出风格示例（仅供模仿语气与长度，不要逐字复述）：\n")
	for _, example := range prompts.Examples {
		b.WriteString("- ")
		b.WriteString(example)
		b.WriteByte('\n')
	}
	return b.String()
}

// buildGeneratorPrompt 组装用户消息。当前只剩群聊一种语境，不再有
// 会话类型 / 私聊昵称 / 近期群活动等分支字段。
//
// 历史已在 Load 出口处自带 `[YYYY-MM-DD HH:MM] ` 前缀，模型自然能看到
// 时间分布，不必再额外注入。
func buildGeneratorPrompt(prompts GeneratorPrompts, now, lastInboundAt time.Time, history []*schema.Message, maxHistoryChars int) string {
	var b strings.Builder
	up := prompts.UserPrompt
	fmt.Fprintf(&b, "%s%s\n", up.CurrentTimeLabel, now.Format(time.RFC3339))
	if !lastInboundAt.IsZero() {
		fmt.Fprintf(&b, "%s%s\n", up.LastInboundAtLabel, lastInboundAt.Format(time.RFC3339))
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

// cleanGeneratedText 清理模型输出并拦截不应发送的片段。
//
// 它只做轻量规则：去掉外层引号、压平空白、检查禁用词；复杂安全判断仍由上游
// guard 和模型提示词负责。forbidden_fragments 这层防御独立于业务路径——即便
// 业务上不再涉及好感/候选/调度等概念，模型也不该说出这些内部词。
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
