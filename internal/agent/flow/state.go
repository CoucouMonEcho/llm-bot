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

import (
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/stats"
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
	// UserID 触发本次消息的用户 ID。
	// stats 中"按人头维度"的参数（好感度、未来的信任度等）都按 UserID 读写
	// 而不是按 SessionID：群聊里一个 SessionID 对应多个 UserID，这些参数
	// 必须跟到人头上才有意义。
	// 私聊虽然一个 session 就一个用户，但调用方统一填充、下游不分流程，
	// 这里对两种会话类型一视同仁都要求填 UserID。
	UserID string
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
// 把"是否拦截"与"为什么拦截"聚合成一个值类型而不是 bool + 两个 string 的组合：
//   - Kind 是分支决策的唯一依据，Detail 仅用于日志；二者的耦合集中在一处，
//     新增一种拦截原因时只需要加一个枚举并填 Detail，不会出现"三字段修改漏一个"
//     的逻辑不自洽；
//   - 值类型（非指针）让零值等价于 Safe——State 起始即放行、拷贝即隔离，
//     调用方不必写任何 nil 判断或显式初始化；
//   - Detail 的含义由 Kind 决定（见下），避免了"空字符串是未检测还是无命中"
//     这种模糊。
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

	// Stats 是本轮对话开始时的人设参数快照（当前含好感度 + 心情，可扩展）。
	// 由 buildMessages 节点在装配 system prompt 之前填入；若该节点没接到
	// LoadStatsFunc（stats 功能关闭）则保持零值 Snapshot{}，此时 PromptLine
	// 只渲染当前时间一行，好感度 / 心情段省略。Graph 其余部分对具体字段无感知。
	Stats stats.Snapshot
}

// NewState 便捷构造器：以 Input 初始化 State，其余字段按零值处理。
func NewState(in *Input) *State {
	return &State{In: in}
}
