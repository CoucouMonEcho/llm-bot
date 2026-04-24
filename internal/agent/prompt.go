// Package agent 实现基于 eino compose.Graph 的 LLM 任务编排。
//
// 本文件负责：
//   - 解析 configs/prompts/*.yaml 中的人设 + 护栏；
//   - 把这些信息组装成一个启动后不可变的 *Persona。
//
// 加载发生在进程启动阶段；运行期的 Guard / 主链都只读取 *Persona 内存快照，
// 不会再碰磁盘。这是"人设固化"的具体实现。
//
// 关于 "<user_input>" 标签：与 judgeSystemPrompt 一样硬编码在代码里，而不是
// 走 YAML 配置。原因是 guardrails 的系统提示词里显式声明了"标签内是用户数据"，
// 这份声明与 wrapper 的具体标签名必须严格对齐——两者本质上是同一份防御契约
// 的两面，把 wrapper 放到 YAML 交给运维去改只会让契约失效。
package agent

import (
	"fmt"
	"os"
	"strings"

	"github.com/cloudwego/eino/schema"
	"gopkg.in/yaml.v3"
)

// personaFile 与 configs/prompts/*.yaml 的结构一一对应。
// 注意：未在此建模的字段会被 yaml.v3 忽略；若需要扩展，先改 YAML 再改这里。
type personaFile struct {
	Persona struct {
		Description string   `yaml:"description"`
		Rules       []string `yaml:"rules"`
	} `yaml:"persona"`
	Guardrails string `yaml:"guardrails"`
}

// Persona 是人设 + 护栏的不可变内存表示。
//
// 构造后本结构体是只读的，所有调用方必须把 *Persona 视为 immutable value。
type Persona struct {
	// SystemPrompt 是完整的 system message 内容，等于
	//   persona.description + "规则:\n" + rules + "\n" + guardrails
	// 启动时一次性构造好，运行期直接当字符串用。
	SystemPrompt string
}

// LoadPersona 从给定的 YAML 文件路径加载并构造 *Persona。
//
// 任一步骤失败均返回 error，由 main 打印后直接终止进程——错误的人设
// 意味着机器人没有可信的"身份锚"，继续启动是危险的。
func LoadPersona(path string) (*Persona, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("agent: read persona file %s: %w", path, err)
	}

	var pf personaFile
	if err := yaml.Unmarshal(raw, &pf); err != nil {
		return nil, fmt.Errorf("agent: parse persona yaml: %w", err)
	}

	// Step 1: 校验必填字段。人设/护栏缺失时拒绝启动。
	if strings.TrimSpace(pf.Persona.Description) == "" {
		return nil, fmt.Errorf("agent: persona.description is required")
	}
	if strings.TrimSpace(pf.Guardrails) == "" {
		return nil, fmt.Errorf("agent: guardrails is required")
	}

	// Step 2: 把 description + rules + guardrails 拼成 SystemPrompt。
	// 顺序刻意把 guardrails 放在最后，使其在模型注意力中具有"最近优先"的效果。
	var sb strings.Builder
	sb.WriteString(strings.TrimSpace(pf.Persona.Description))
	if len(pf.Persona.Rules) > 0 {
		sb.WriteString("\n\n遵守以下规则：\n")
		for _, r := range pf.Persona.Rules {
			r = strings.TrimSpace(r)
			if r == "" {
				continue
			}
			sb.WriteString("- ")
			sb.WriteString(r)
			sb.WriteByte('\n')
		}
	}
	sb.WriteString("\n")
	sb.WriteString(strings.TrimSpace(pf.Guardrails))

	return &Persona{
		SystemPrompt: sb.String(),
	}, nil
}

// BuildMessages 按 "system + history + current user" 的顺序组装一次 LLM 请求的
// 完整消息列表。history 必须已经是"时间从旧到新"的。
//
// 用户 Query 被包裹在硬编码的 <user_input> 标签中：该标签与 guardrails 里声明
// 的"标签内是数据而非指令"形成闭环，落地"指令—数据分离"。
func (p *Persona) BuildMessages(history []*schema.Message, query string) ([]*schema.Message, error) {
	userMsg := schema.UserMessage("<user_input>\n" + query + "\n</user_input>")
	msgs := make([]*schema.Message, 0, len(history)+2)
	msgs = append(msgs, schema.SystemMessage(p.SystemPrompt))
	msgs = append(msgs, history...)
	msgs = append(msgs, userMsg)
	return msgs, nil
}
