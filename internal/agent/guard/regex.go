// Package guard 实现提示词注入防护复合节点。
//
// 本文件是"第一级防线"：同步运行的正则黑名单。
// 命中即视为攻击，立即降级；不调用任何 LLM。
//
// 正则的维护原则：
//  1. 宁可漏报、不可误伤——被误判为攻击的正常对话会直接收到降级回复，
//     用户体验极差。因此规则要追求"高精确率"而非"高召回率"；
//  2. 模式应当对常见绕过形式做一定容忍，例如 [\s\S]+?、(?i) 大小写不敏感；
//  3. 模式的演进由 configs/config.yaml 的 guard.regex_patterns 驱动，
//     不要把正则硬编码到代码里——这样维护者调整黑名单时只需改配置后重启，
//     无需改动 Go 代码。当前版本不支持运行时热加载。
package guard

import (
	"fmt"
	"regexp"
)

// RegexMatcher 是一组已编译好的正则的集合。
type RegexMatcher struct {
	patterns []*regexp.Regexp
}

// NewRegexMatcher 把字符串形式的正则列表编译为 RegexMatcher。
// 任一条编译失败整体失败——错误的防御规则比没有防御更糟糕
// （会让防御看起来生效但实际上缺失某些模式）。
func NewRegexMatcher(rawPatterns []string) (*RegexMatcher, error) {
	patterns := make([]*regexp.Regexp, 0, len(rawPatterns))
	for i, p := range rawPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("guard: compile regex[%d] %q: %w", i, p, err)
		}
		patterns = append(patterns, re)
	}
	return &RegexMatcher{patterns: patterns}, nil
}

// Match 检查 text 是否命中任一正则。
// 命中则返回命中的原模式字符串，用于日志观测；否则返回 ""。
func (m *RegexMatcher) Match(text string) (hit string, matched bool) {
	for _, re := range m.patterns {
		if re.MatchString(text) {
			return re.String(), true
		}
	}
	return "", false
}
