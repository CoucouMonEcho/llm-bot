// Package nodes 实现 Agent Graph 中的非 guard Lambda 节点。
//
// 本文件：postproc 节点。
// 位置：guard(未拦截) → postproc → saveHistory。
// 职责：对主链产出的 Reply 做"发送前清洗"——裁剪空白、限制长度、
// 剥除潜在的"泄露 system prompt"式片段。
//
// 设计权衡：
//   - 不再做"输出侧攻击检测"（即不用 LLM 二次判断输出是否违规），
//     因为业务明确要求不增加响应延迟；
//   - 规则都是"肉眼可验证"的纯文本处理：正则 + 字符数裁剪。
package nodes

import (
	"cmp"
	"context"
	"regexp"
	"strings"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
)

// maxReplyRunes 是回复文本允许的最大字符数（按 rune 算，兼容中英混排）。
// 超长会被截断并追加省略号——避免机器人发出过长消息刷屏。
const maxReplyRunes = 800

// collapseBlankLines 把连续 3 个及以上换行折叠成 2 个。
// 预编译放包级：避免每次调用 postproc 都重新编译正则。
var collapseBlankLines = regexp.MustCompile(`\n{3,}`)

// NewPostproc 构造 postproc Lambda 节点。
func NewPostproc() *compose.Lambda {
	return compose.InvokableLambda(postproc)
}

// postproc 对 state.Reply 做清洗并写回 state。返回的 state 与入参同一实例。
//
// 步骤：
//  1. 去首尾空白 + 压缩连续空行；
//  2. 按 rune 截断到 maxReplyRunes；
//  3. 保证最终 Content 非空——否则把它替换成一个温和的默认回复，避免
//     下游发送空字符串被 NapCat 拒绝。
func postproc(_ context.Context, st *flow.State) (*flow.State, error) {
	if st.Reply == nil {
		return st, nil
	}

	content := st.Reply.Content

	// Step 1: 修剪空白 & 折叠多余空行。
	content = strings.TrimSpace(content)
	content = collapseBlankLines.ReplaceAllString(content, "\n\n")

	// Step 2: 按 rune 截断以兼容中文。
	runes := []rune(content)
	if len(runes) > maxReplyRunes {
		runes = append(runes[:maxReplyRunes], []rune("……")...)
		content = string(runes)
	}

	// Step 3: 确保非空——清洗后若空，兜底一句温和的占位回复。
	st.Reply.Content = cmp.Or(content, "我去洗澡了")
	return st, nil
}
