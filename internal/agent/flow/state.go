// Package flow 定义 Agent Graph 内部流转的共享类型。
//
// 独立成包的理由：
//   - agent 顶层会导入 guard 与 nodes 来装配 Graph；
//   - guard 与 nodes 需要共享 Input / State / VerdictKind 定义；
//   - 若把这些类型放在 agent 顶层，guard / nodes 导入 agent 会形成循环；
//   - 因此提取为一个"叶子包"，agent / guard / nodes 都单向依赖 flow。
//
// 本包仅保存**类型定义**，不放任何业务逻辑。
package flow

import (
	"github.com/cloudwego/eino/schema"
)

// Input 是从 Bot 主循环喂进 Graph 的入参。
// 字段设计刻意精简——只保留 Agent 层真正消费的最小集。
type Input struct {
	// Platform 消息来源平台的字符串标识（例如 "onebot"）。
	// stats 按"平台 + 用户"维度隔离好感度：将来接入微信 / Telegram 时，
	// 同号 userID 不会被误判为同一人。
	// 用 string 而不是 domain.Platform：避免 flow 反向依赖 domain，
	// 让 flow 保持为纯"Graph 管道类型"的叶子包。调用方（bot.handle）
	// 传 string(m.Platform) 即可。
	Platform string
	// SessionID 对话的唯一标识，用于 Redis key 和日志。
	SessionID string
	// ConvType 会话类型的字符串标识（例如 "private" / "group"）。
	// Agent 层只用它判断是否需要群聊双写，不反向依赖 domain 包。
	ConvType string
	// UserID 触发本次消息的用户 ID。
	// stats 中"按人头维度"的参数（好感度、未来的信任度等）都按 UserID 读写
	// 而不是按 SessionID：群聊里一个 SessionID 对应多个 UserID，这些参数
	// 必须跟到人头上才有意义。
	// 私聊虽然一个 session 就一个用户，但调用方统一填充、下游不分流程，
	// 这里对两种会话类型一视同仁都要求填 UserID。
	UserID string
	// Query 用户的原始文本（已经去掉 @、前缀等触发标记）。
	Query string
}

// VerdictKind 是一次防护判定的种类。零值 VerdictSafe 代表"未被任何防线拦截"，
// 这使得 State 零值即等价于"放行"，Graph 的起始节点无需显式初始化。
//
// 拦截理由的细节（命中的 pattern、裁判输出等）由产生它的节点当场打日志，
// 不再随 State 流转——细节只对当时排查的人有用，外化成字段反而会让 Graph
// 上下游需要一起维护"何时填空、何时为空"的约定。
type VerdictKind int

const (
	// VerdictSafe 未被任何防线拦截——正常走主链并落历史。
	VerdictSafe VerdictKind = iota
	// VerdictRegex 被第一级同步正则黑名单命中。
	VerdictRegex
	// VerdictJudge 被第二级 LLM 裁判判定为攻击。
	VerdictJudge
)

// String 返回 Kind 的稳定字符串，便于日志检索。
func (k VerdictKind) String() string {
	switch k {
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

	// History 是 loadContext 节点从 Redis 拉回的历史消息（旧→新）。
	// 读取失败时为 nil 切片；下游应当视 nil 与空切片等价。
	History []*schema.Message

	// Messages 是 buildMessages 节点组装好的"system + history + current"消息列表，
	// 直接交给 chatModel 节点作为 Generate 的入参。
	Messages []*schema.Message

	// Reply 是 LLM 的最终回复（可能为降级回复）。
	// 主链正常返回或 fallback 节点生成后填充。
	Reply *schema.Message

	// VerdictKind 是本轮防护判定的种类。
	// 零值即 VerdictSafe，表示一路放行；非零意味着 Graph 应路由到 fallback。
	VerdictKind VerdictKind

	// Affinity / Mood 是本轮对话开始时 stats 快照的平铺字段。
	// 由 loadContext 节点在装配系统提示词之前填入；若 stats 功能关闭、读取
	// 失败或 UserID 缺失，两者均保持零值——下游按"无信号"处理（具体语义仍
	// 在 stats 包定义，零值即 stats.Snapshot.IsZero() 的状态）。
	//
	// 平铺成 int 而不是嵌入 stats.Snapshot：让 flow 包不再依赖 stats（stats
	// 还会反向引用 flow 之外的能力），buildMessages 等节点也只搬运标量。
	Affinity int
	Mood     int

	// Memory 是本轮对话开始时读取到的长期用户事实摘要。
	// 它按"平台 + 用户"维度存储，和会话历史分离：history 负责最近发生的逐条对话，
	// Memory 负责跨会话保留高度压缩的偏好、近况与雷区。为空表示无可用记忆。
	Memory string
}

// NewState 便捷构造器：以 Input 初始化 State，其余字段按零值处理。
func NewState(in *Input) *State {
	return &State{In: in}
}
