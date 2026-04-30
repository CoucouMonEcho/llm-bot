// Package nodes 实现 Agent Graph 中的回复后处理节点。
//
// 本文件：postproc 节点。
// 位置：chatModel → postproc → saveHistory → updateMemory → scoreStats。
// 职责：对主链产出的 Reply 做"发送前清洗"——裁剪空白、限制长度、
// 剥除潜在的"泄露 system prompt"式片段。
//
// 设计权衡：
//   - 只做"肉眼可验证"的纯文本处理：正则折叠空行 + 按 rune 截断长度；
//     规则本地即可审查，零 IO、零 LLM 调用，处理耗时几乎不可测；
//   - 输出侧不做"LLM 二次判断回复是否违规"——业务明确要求不增加响应延迟，
//     注入/越狱的判定放在 judgeGate 节点由独立 judge 在输入侧前置完成。
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
func NewPostproc(emptyReplyFallback string) *compose.Lambda {
	return compose.InvokableLambda(func(ctx context.Context, st *flow.State) (*flow.State, error) {
		return postproc(ctx, st, emptyReplyFallback)
	})
}

// postproc 对 state.Reply 做清洗并写回 state。返回的 state 与入参同一实例。
//
// 步骤：
//  1. 去首尾空白 + 压缩连续空行；
//  2. 按 rune 截断到 maxReplyRunes；
//  3. 保证最终 Content 非空——否则把它替换成一个温和的默认回复，避免
//     下游发送空字符串被 NapCat 拒绝。
func postproc(_ context.Context, st *flow.State, emptyReplyFallback string) (*flow.State, error) {
	if st.Reply == nil {
		return st, nil
	}

	content := st.Reply.Content

	// 第一步：修剪空白并折叠多余空行。
	content = strings.TrimSpace(content)
	content = collapseBlankLines.ReplaceAllString(content, "\n\n")

	// 第二步：按 rune 截断以兼容中文。
	runes := []rune(content)
	if len(runes) > maxReplyRunes {
		runes = append(runes[:maxReplyRunes], []rune("……")...)
		content = string(runes)
	}

	// 第三步：确保非空——清洗后若空，兜底一句温和的占位回复。
	st.Reply.Content = cmp.Or(content, emptyReplyFallback)
	return st, nil
}
