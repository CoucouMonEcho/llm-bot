package proactive

import (
	"fmt"
	"strings"

	"github.com/echo/llm-bot/internal/llmtext"
)

// GeneratorPrompts 是主动消息生成器的文本契约。
//
// 加载一次即固化到内存。当前主动消息只面向群聊，因此 user prompt 由
// "当前时间 + 群里上次活跃时间 + 历史"三块组成。
//
// Examples 只保存"期望输出长什么样"的 assistant 正例字符串列表；运行期会被
// 渲染成 system prompt 里的结构化 few-shot 段落，而不是伪造多轮 user/assistant
// 消息——后者会让模型把 synthetic user 误读为当前对话上下文。允许为空，便于
// 退化到纯规则 prompt。
type GeneratorPrompts struct {
	System             string
	Examples           []string
	ForbiddenFragments []string
	UserPrompt         GeneratorUserPrompt
}

// GeneratorUserPrompt 列出渲染主动开场白用户消息的全部 label。
//
// 字段会在 normalized 时校验非空——所有 label 都是契约的一部分，缺一项就
// 让装配期直接失败，避免运行期才发现 prompt 拼出空字符串。
type GeneratorUserPrompt struct {
	CurrentTimeLabel   string
	LastInboundAtLabel string
	HistoryHeader      string
	NoHistoryText      string
	Closing            string
}

func LoadGeneratorPrompts(path string) (GeneratorPrompts, error) {
	raw, err := llmtext.LoadPromptFile(path, "proactive generator")
	if err != nil {
		return GeneratorPrompts{}, err
	}

	prompts, err := parseGeneratorMarkdown(raw)
	if err != nil {
		return GeneratorPrompts{}, fmt.Errorf("proactive: parse generator prompt: %w", err)
	}
	prompts, err = prompts.normalized()
	if err != nil {
		return GeneratorPrompts{}, fmt.Errorf("proactive: invalid generator prompts: %w", err)
	}
	return prompts, nil
}

func parseGeneratorMarkdown(raw string) (GeneratorPrompts, error) {
	sections := splitMarkdownSections(raw)
	up, err := parseGeneratorUserPrompt(sections["用户提示模板"])
	if err != nil {
		return GeneratorPrompts{}, err
	}
	return GeneratorPrompts{
		System:             sections["系统提示"],
		Examples:           parseBulletList(sections["输出示例"]),
		ForbiddenFragments: parseBulletList(sections["禁用片段"]),
		UserPrompt:         up,
	}, nil
}

// splitMarkdownSections 按 generator prompt 的二级标题切分固定小节。
func splitMarkdownSections(raw string) map[string]string {
	sections := make(map[string]string)
	var current string
	var lines []string
	flush := func() {
		if current != "" {
			sections[current] = strings.TrimSpace(strings.Join(lines, "\n"))
		}
	}
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			flush()
			current = strings.TrimSpace(strings.TrimPrefix(trimmed, "## "))
			lines = lines[:0]
			continue
		}
		if current != "" {
			lines = append(lines, line)
		}
	}
	flush()
	return sections
}

func parseBulletList(section string) []string {
	var out []string
	for _, line := range strings.Split(section, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "- ") {
			item := strings.TrimSpace(strings.TrimPrefix(line, "- "))
			item = strings.Trim(item, "\"“”")
			if item != "" {
				out = append(out, item)
			}
		}
	}
	return out
}

func parseGeneratorUserPrompt(section string) (GeneratorUserPrompt, error) {
	values := make(map[string]string)
	for _, item := range parseBulletList(section) {
		key, value, ok := strings.Cut(item, ":")
		if !ok {
			return GeneratorUserPrompt{}, fmt.Errorf("generator.user_prompt item %q must use key: value", item)
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return GeneratorUserPrompt{
		CurrentTimeLabel:   values["current_time_label"],
		LastInboundAtLabel: values["last_inbound_at_label"],
		HistoryHeader:      values["history_header"],
		NoHistoryText:      values["no_history_text"],
		Closing:            values["closing"],
	}, nil
}

func (p GeneratorPrompts) normalized() (GeneratorPrompts, error) {
	p.System = strings.TrimSpace(p.System)
	if p.System == "" {
		return GeneratorPrompts{}, fmt.Errorf("系统提示 section is required")
	}

	p.ForbiddenFragments = normalizeList(p.ForbiddenFragments)
	if len(p.ForbiddenFragments) == 0 {
		return GeneratorPrompts{}, fmt.Errorf("禁用片段 section must contain at least one entry")
	}

	// examples 是可选项：为空时主动消息回退到纯规则 prompt，不报错。
	p.Examples = normalizeList(p.Examples)

	up := &p.UserPrompt
	required := []struct {
		name  string
		value *string
	}{
		{"用户提示模板 current_time_label", &up.CurrentTimeLabel},
		{"用户提示模板 last_inbound_at_label", &up.LastInboundAtLabel},
		{"用户提示模板 history_header", &up.HistoryHeader},
		{"用户提示模板 no_history_text", &up.NoHistoryText},
		{"用户提示模板 closing", &up.Closing},
	}
	for _, field := range required {
		*field.value = strings.TrimSpace(*field.value)
		if *field.value == "" {
			return GeneratorPrompts{}, fmt.Errorf("%s is required", field.name)
		}
	}
	return p, nil
}

func normalizeList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
