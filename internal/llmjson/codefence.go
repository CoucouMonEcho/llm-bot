// Package llmjson 收敛若干 LLM JSON 输出处理小工具。
//
// 项目里多处需要剥掉模型输出可能携带的 Markdown 代码块包裹，
// 单独成包避免在 stats / memory 之间制造业务方向的反向依赖。
package llmjson

import "strings"

// StripFence 去掉可能存在的 ```...``` 包裹；兼容 ```json 等带语言标签的前缀。
//
// 模型有时无视"不要代码块"指令，做一次轻量兜底就够，不追求完美。
func StripFence(s string) string {
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
