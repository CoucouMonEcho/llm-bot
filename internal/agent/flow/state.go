// Package flow 定义 Agent Graph 内部流转的共享类型。
//
// 独立成包的理由：
//   - agent 顶层会导入 guard 与 nodes 来装配 Graph；
//   - guard 与 nodes 需要共享 Input / State / Verdict 定义；
//   - 若把这些类型放在 agent 顶层，guard / nodes 导入 agent 会形成循环；
//   - 因此提取为一个"叶子包"，agent / guard / nodes 都单向依赖 flow。
//
// 本包仅保存**类型定义**，不放任何业务逻辑。
package flow

import "github.com/cloudwego/eino/schema"

// Input 是从 Bot 主循环喂进 Graph 的入参。
// 字段设计刻意精简——平台相关字段留在 domain.InboundMessage 里，
// 不进入 Graph。
type Input struct {
	// SessionID 对话的唯一标识，用于 Redis key 和日志。
	SessionID string
	// Query 用户的原始文本（已经去掉 @、前缀等触发标记）。
	Query string
	// UserName 触发用户的昵称。当前 Persona.BuildMessages 尚未消费此字段，
	// 保留以便未来把称呼注入 system prompt 或少样例（Few-shot）时使用。
	UserName string
}

// VerdictKind 是一次防护判定的种类。零值 VerdictSafe 代表"未被任何防线拦截"，
// 这使得 State 零值即等价于"放行"，Graph 的起始节点无需显式初始化。
type VerdictKind int

const (
	// VerdictSafe 未被任何防线拦截——正常走主链并落历史。
	VerdictSafe VerdictKind = iota
	// VerdictRegex 被第一级同步正则黑名单命中。
	VerdictRegex
	// VerdictJudge 被第二级 LLM 裁判判定为攻击。
	VerdictJudge
)

// Verdict 是一次防护判定的完整值对象。
//
// 为什么不再用 bool + 两个 string：
//   - 旧 State 里 Blocked/BlockedBy/HitDetail 三字段耦合——任何一处修改都要
//     同步更新三个字段，缺一个就逻辑不自洽；
//   - 零值语义模糊："" 到底是"未检测"还是"检测后无命中"？
//   - Verdict 是值类型（非指针），零值即 Safe，拷贝即隔离，不会出现 nil 判断。
type Verdict struct {
	// Kind 判定种类。仅此字段参与分支决策。
	Kind VerdictKind
	// Detail 判定的附加信息，仅用于日志：
	//   - Kind==VerdictRegex 时存命中的正则 pattern 原文；
	//   - Kind==VerdictJudge 时可选地保留裁判的原始输出；
	//   - Kind==VerdictSafe 时为空。
	Detail string
}

// Blocked 当前判定是否要走降级分支。Graph 的 verdict branch 用它做路由。
func (v Verdict) Blocked() bool { return v.Kind != VerdictSafe }

// String 返回 Kind 的稳定字符串，便于日志检索。
func (v Verdict) String() string {
	switch v.Kind {
	case VerdictSafe:
		return "safe"
	case VerdictRegex:
		return "regex"
	case VerdictJudge:
		return "judge"
	default:
		return "unknown"
	}
}

// State 是在 Graph 节点间流转的聚合状态。
// 每个节点读取需要的字段，写入自己产出的字段；按"管道灌水"方式传递。
//
// 之所以不让每个节点有自己独立的 input/output 类型，是因为
// eino Graph 需要边两端类型匹配；用单一 State 走全程可以省去大量
// 手写适配层，同时让节点可以自由获取上游信息（例如 saveHistory 需要
// 同时知道原始 Query 和最终 Reply）。
type State struct {
	// In 是入参的只读快照，整个 Graph 生命周期内不应被修改。
	In *Input

	// History 是 loadHistory 节点从 Redis 拉回的历史消息（旧→新）。
	// 读取失败时为 nil 切片；downstream 应当视 nil 与空切片等价。
	History []*schema.Message

	// Messages 是 buildMessages 节点组装好的"system + history + current"消息列表，
	// 直接交给 guardedModel 节点作为 Generate 的入参。
	Messages []*schema.Message

	// Reply 是 LLM 的最终回复（可能为降级回复）。
	// 主链正常返回或 fallback 节点生成后填充。
	Reply *schema.Message

	// Verdict 聚合了"是否拦截"与"为什么拦截"。
	// 零值即 VerdictSafe，表示一路放行。
	Verdict Verdict
}

// NewState 便捷构造器：以 Input 初始化 State，其余字段按零值处理。
func NewState(in *Input) *State {
	return &State{In: in}
}
