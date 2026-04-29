package proactive

import (
	"fmt"
	"os"
	"strings"

	"github.com/echo/llm-bot/internal/domain"
	"gopkg.in/yaml.v3"
)

type generatorPromptsFile struct {
	Generator GeneratorPrompts `yaml:"generator"`
}

// GeneratorPrompts contains the text contracts used by the proactive generator.
// Values are loaded once at startup and treated as immutable afterwards.
type GeneratorPrompts struct {
	System             string              `yaml:"system"`
	ForbiddenFragments []string            `yaml:"forbidden_fragments"`
	UserPrompt         GeneratorUserPrompt `yaml:"user_prompt"`
}

type GeneratorUserPrompt struct {
	CurrentTimeLabel         string            `yaml:"current_time_label"`
	ConversationTypeLabel    string            `yaml:"conversation_type_label"`
	PrivateDisplayNameLabel  string            `yaml:"private_display_name_label"`
	LastInboundAtLabel       string            `yaml:"last_inbound_at_label"`
	RecentGroupActivityLabel string            `yaml:"recent_group_activity_label"`
	HistoryHeader            string            `yaml:"history_header"`
	NoHistoryText            string            `yaml:"no_history_text"`
	Closing                  string            `yaml:"closing"`
	ConversationTypes        map[string]string `yaml:"conversation_types"`
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
		{"generator.user_prompt.conversation_type_label", &up.ConversationTypeLabel},
		{"generator.user_prompt.private_display_name_label", &up.PrivateDisplayNameLabel},
		{"generator.user_prompt.last_inbound_at_label", &up.LastInboundAtLabel},
		{"generator.user_prompt.recent_group_activity_label", &up.RecentGroupActivityLabel},
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

	p.UserPrompt.ConversationTypes = normalizeMap(p.UserPrompt.ConversationTypes)
	for _, key := range []string{string(domain.ConversationGroup), string(domain.ConversationPrivate)} {
		if p.UserPrompt.ConversationTypes[key] == "" {
			return GeneratorPrompts{}, fmt.Errorf("generator.user_prompt.conversation_types.%s is required", key)
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

func normalizeMap(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			out[key] = value
		}
	}
	return out
}
