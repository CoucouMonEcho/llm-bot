package proactive

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type generatorPromptsFile struct {
	Generator GeneratorPrompts `yaml:"generator"`
}

// GeneratorPrompts 是主动消息生成器的文本契约。
//
// 加载一次即固化到内存，运行期不再 IO。当前主动消息只面向群聊，因此 user
// prompt 只剩"当前时间 + 群里上次互动时间 + 历史"三块。
type GeneratorPrompts struct {
	System             string              `yaml:"system"`
	ForbiddenFragments []string            `yaml:"forbidden_fragments"`
	UserPrompt         GeneratorUserPrompt `yaml:"user_prompt"`
}

// GeneratorUserPrompt 列出渲染主动开场白用户消息的全部 label。
//
// 字段会在 normalized 时校验非空——所有 label 都是契约的一部分，缺一项就
// 让装配期直接失败，避免运行期才发现 prompt 拼出空字符串。
type GeneratorUserPrompt struct {
	CurrentTimeLabel   string `yaml:"current_time_label"`
	LastInboundAtLabel string `yaml:"last_inbound_at_label"`
	HistoryHeader      string `yaml:"history_header"`
	NoHistoryText      string `yaml:"no_history_text"`
	Closing            string `yaml:"closing"`
}

func LoadGeneratorPrompts(path string) (GeneratorPrompts, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return GeneratorPrompts{}, fmt.Errorf("proactive: read prompts file %s: %w", path, err)
	}

	var pf generatorPromptsFile
	if err := yaml.Unmarshal(raw, &pf); err != nil {
		return GeneratorPrompts{}, fmt.Errorf("proactive: parse prompts yaml: %w", err)
	}
	prompts, err := pf.Generator.normalized()
	if err != nil {
		return GeneratorPrompts{}, fmt.Errorf("proactive: invalid generator prompts: %w", err)
	}
	return prompts, nil
}

func (p GeneratorPrompts) normalized() (GeneratorPrompts, error) {
	p.System = strings.TrimSpace(p.System)
	if p.System == "" {
		return GeneratorPrompts{}, fmt.Errorf("generator.system is required")
	}

	p.ForbiddenFragments = normalizeList(p.ForbiddenFragments)
	if len(p.ForbiddenFragments) == 0 {
		return GeneratorPrompts{}, fmt.Errorf("generator.forbidden_fragments must contain at least one entry")
	}

	up := &p.UserPrompt
	required := []struct {
		name  string
		value *string
	}{
		{"generator.user_prompt.current_time_label", &up.CurrentTimeLabel},
		{"generator.user_prompt.last_inbound_at_label", &up.LastInboundAtLabel},
		{"generator.user_prompt.history_header", &up.HistoryHeader},
		{"generator.user_prompt.no_history_text", &up.NoHistoryText},
		{"generator.user_prompt.closing", &up.Closing},
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
