// Package llmtext 提供 LLM prompt 与模型输出文本的小工具。
package llmtext

import (
	"fmt"
	"os"
	"strings"
)

// LoadPromptFile 读取 prompt 文件并返回去掉首尾空白后的文本。
func LoadPromptFile(path, name string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s: read prompt file %s: %w", name, path, err)
	}
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return "", fmt.Errorf("%s: prompt is empty", name)
	}
	return text, nil
}

// StripCodeFence 去掉可能存在的 Markdown 代码块包裹。
func StripCodeFence(s string) string {
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```")
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	s = strings.TrimSuffix(strings.TrimSpace(s), "```")
	return strings.TrimSpace(s)
}
