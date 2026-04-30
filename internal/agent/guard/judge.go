// Package guard 的 judge.go 实现基于独立 LLM 的注入攻击分类器。
//
// 为什么用独立模型：
//  1. 主模型可能被人设 system prompt 引导去"扮演角色"，在面对注入时判断力弱；
//     裁判模型使用中性的"安全分类"system prompt，专注于判断"这段文字是否
//     是一个针对大模型的注入/越狱/角色重置攻击"。
//  2. 可以选择更便宜的小模型做裁判（例如 gpt-4o-mini / deepseek-chat），
//     大幅降低每次双倍调用的成本。
//
// 设计要点：
//   - 裁判 prompt 在启动时从 YAML 加载并固化到内存，运行期不再 IO；
//   - 裁判只输出 "safe" / "attack" 两个 token，极短回复节省 token 费用；
//   - 模型输出不可完全信任：如果出现既不是 safe 也不是 attack 的字符串，
//     一律按"非 safe"处理。删掉同步正则层后，只有明确 safe 才能进入主链。
package guard

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"gopkg.in/yaml.v3"
)

// 裁判内部的两个判定 token。模型只能输出这两个字符串之一；
// 其余任何输出都按"非 safe"处理。
//
// 故意不把它们暴露成导出常量——对上层而言 Classify 只关心能否放行。
const (
	judgeTokenSafe   = "safe"
	judgeTokenAttack = "attack"
)

type judgePromptFile struct {
	System string `yaml:"system"`
}

// LoadJudgePrompt 读取裁判模型的 system prompt。
func LoadJudgePrompt(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("guard: read judge prompt file %s: %w", path, err)
	}
	var pf judgePromptFile
	if err := yaml.Unmarshal(raw, &pf); err != nil {
		return "", fmt.Errorf("guard: parse judge prompt yaml: %w", err)
	}
	system := strings.TrimSpace(pf.System)
	if system == "" {
		return "", fmt.Errorf("guard: judge prompt system is required")
	}
	return system, nil
}

// Judge 是一个并发安全的裁判。构造一次，全局复用。
type Judge struct {
	model        model.BaseChatModel
	systemPrompt string
}

// NewJudge 把一个 ChatModel 包装成 Judge。
func NewJudge(m model.BaseChatModel, systemPrompt string) *Judge {
	return &Judge{model: m, systemPrompt: systemPrompt}
}

// Classify 对 input 做一次攻击分类。
//
// 注意传入的 ctx：在 judgeGate 节点里，这个 ctx 由 Graph 调用链传入；
// 上游取消时裁判请求也会一起取消。裁判只负责输入侧判定，不与主聊天模型并发。
//
// 返回值语义：
//   - safe==true 仅表示模型明确输出 safe，调用方可以进入主链；
//   - safe==false 表示 attack、未知输出、空输出或其它非 safe 文本；
//   - 网络错误 / ctx cancel 时返回 (false, err)，调用方按 fail-closed 处理。
//
// 选择 bool 而非枚举：删掉正则层后，业务只需要回答"能否放行"。
func (j *Judge) Classify(ctx context.Context, input string) (safe bool, err error) {
	messages := []*schema.Message{
		schema.SystemMessage(j.systemPrompt),
		schema.UserMessage("<input>\n" + input + "\n</input>"),
	}
	msg, err := j.model.Generate(ctx, messages)
	if err != nil {
		return false, err
	}
	// 规范化模型输出——去空白、转小写、剥离常见引号/标点。
	content := strings.ToLower(strings.TrimSpace(msg.Content))
	content = strings.Trim(content, "\"'. \n\t")

	switch content {
	case judgeTokenSafe:
		return true, nil
	case judgeTokenAttack:
		return false, nil
	default:
		// 未预期的输出不能再放行；删掉正则层后，只有明确 safe 才进主链。
		return false, nil
	}
}
