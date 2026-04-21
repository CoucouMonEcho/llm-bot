// Package flow 定义 Agent Graph 内部流转的共享类型。
//
// 独立成包的理由：
//   - agent 顶层会导入 guard 与 nodes 来装配 Graph；
//   - guard 与 nodes 需要共享 Input / State 定义；
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
	// UserName 触发用户的昵称，目前仅作可选 prompt 变量使用。
	UserName string
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

	// Reply 是 LLM 的最终回复（可能为降级回复）。
	// 在 guard 内部主链完成或 fallback 节点生成后填充。
	Reply *schema.Message

	// Blocked 标记本条消息是否被判定为攻击并走了降级分支。
	// - true：guard 节点产出时 Reply 可能为 nil，fallback 节点负责填充；
	//   同时 saveHistory 应当跳过此条消息与其降级回复。
	// - false：正常走主链，postproc → saveHistory。
	Blocked bool

	// BlockedBy 记录判定为攻击的防线来源："regex" / "judge" / ""。
	// 仅用于日志和可观测性，不参与业务判定。
	BlockedBy string

	// HitDetail 用于日志：对 regex 存命中的模式字符串，
	// 对 judge 存裁判模型的原始判定字符串。
	HitDetail string
}

// NewState 便捷构造器：以 Input 初始化 State，Reply 等字段按零值处理。
func NewState(in *Input) *State {
	return &State{In: in}
}
