// Package stats 管理机器人运行期会影响回复风格的"人设参数"。
//
// 当前包含两个参数：
//   - Affinity（好感度）：按"平台 + 用户"分 field，存在全局 Hash
//     "bot_stats_affinity" 里，field 形如 "<platform>_<userID>"。
//     语义是"某个用户在机器人心里的累计好感度"。按 hash field 分开而不是
//     每人一把 key，是为了方便后续做"好感度排行榜" / 批量读写运营动作，
//     同时也让所有用户共享同一把 key 的 TTL 策略（永不过期）。
//   - Mood（心情）：全局 Hash "bot_stats_global"，field "mood"，
//     语义是"机器人当前整体心情"，所有用户共享——人不会因为换一个聊天对象
//     就切换心情。放在 hash 里而不是独立 string key，是为了给后续扩展的
//     "其他全局变量"（疲劳度、在线时长、连续对话计数……）留好位置，
//     每增加一个全局量就是 hash 里多一个 field，不用再开新的 key。
//
// TTL 策略：两把 stats hash 都永不过期——人设是长期累积的结果，
// 用户"沉寂 30 天再回来"时理应记得他们之前惹没惹过机器人。与 history
// key 的滑动过期策略（见 store.history.go）刻意相反。
//
// 心情的"自然衰减"：人情绪上头总会冷静回来，同一个模型应体现在 mood 上。
// 实现方式是让 mood 每过 moodRegressionInterval（10 分钟）就朝 0 方向回归 1 点，
// 上限在 0——从正面回落不会穿到负面、反之亦然。衰减不用定时任务，而是在
// "下一次真正要写 mood"的一刻懒结算（见 Store.applyMood）：
//   - 省掉一个独立的后台 goroutine / 定时器；
//   - 不活跃时段不产生任何 Redis 流量；
//   - 读路径（Snapshot）保持为纯存储读，不做虚拟回归——下一轮 Dispatch 自然会
//     把积压的衰减结算回来，最多让 persona prompt 里的数字"落后一轮对话"，
//     这点漂移相对于实现复杂度是划算的。
//
// 为此 keyGlobal 里额外存一个 fieldLastChatAt（Unix 秒），记录上次 mood 写入的
// 时刻；在 applyMood 结算完成后顺带刷新。
//
// 两个参数都是整数，0 代表中性，正负对称。Redis 缺 field 时 HINCRBY 从 0
// 起步，恰好等价于"从中性开始累积"，不需要任何预初始化。
//
// 扩展参数（疲劳度 / 信任度 / 饱腹感 ...）时，改动集中在本文件：
//  1. 在 Snapshot / Delta 结构体里加字段；
//  2. 在 Store.Snapshot / Store.Apply 里把字段加到对应 Hash（全局量进
//     keyGlobal、按人量进 keyAffinity 那样的独立 hash 或同一 hash 的新 field）；
//  3. 在 scoreSystemPrompt 和 scoreResp 里加对应的 JSON 字段；
//  4. 在 Snapshot.PromptLine 里把新字段渲染进 system prompt。
//
// 其他地方（flow.State / Persona.BuildMessages / nodes / bot / main）对具体
// 字段不感知——它们只搬运 Snapshot / Delta 这两个值类型。
//
// 设计约束：
//  1. 无 L2 内存缓存：Redis 是唯一真相源，避免多实例部署时的缓存一致性问题；
//  2. fail-soft：读写任何失败都不应阻断主对话——读失败返回零值继续，
//     写失败只打日志不上抛；
//  3. 打分链路完全异步：Score + Apply 由 Dispatch 起独立 goroutine 执行，
//     主回复路径不承担任何额外延迟。
//
// 本包不感知 Enabled 开关——是否调用 Dispatch 由上层（bot / agent 装配处）决定。
package stats

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"github.com/redis/go-redis/v9"
)

// 硬编码的边界与常量。
//
// 边界是 stats 语义模型的一部分（与打分 prompt 里声明的 delta 范围一一对应），
// 允许运维在 YAML 里改动会让打分契约与存储边界脱节，因此刻意不做配置化。
const (
	// Affinity 范围：0 为中性，正为喜欢，负为讨厌。
	affMin, affMax = -100, 100

	// Mood 范围：0 为平静，正为愉悦，负为烦躁。
	moodMin, moodMax = -50, 50

	// 单轮打分的 delta 上限，与 Score prompt 里声明的范围保持一致；
	// 解析后再 clamp 一次作为双保险，防止模型无视指令越界输出。
	affDeltaMax  = 3
	moodDeltaMax = 2

	// Dispatch 独立 ctx 的超时。10s 足够一次 judge 模型调用 + 一次 Redis 写。
	dispatchTimeout = 10 * time.Second

	// keyGlobal 存所有"与具体用户无关"的人设参数——当前有 mood 与
	// last_chat_at，后续可以在同一个 hash 里加新 field（疲劳度、连续对话计数……）。
	keyGlobal = "bot_stats_global"
	// fieldMood 是 keyGlobal 中保存心情数值的 field 名。
	// 单独抽常量避免拼写漂移：改名只动这一行。
	fieldMood = "mood"
	// fieldLastChatAt 是 keyGlobal 中保存"上次心情写入时刻"的 field。
	// 存 Unix 秒而非 RFC3339 字符串：规避时区 / 闰秒的解析坑，整型也便于
	// Redis 内部比较和未来做 ZADD 之类的时间窗操作。
	//
	// 命名上留"chat_at"而非"mood_at"，是为了给将来"会话计数 / 活跃时段"
	// 等附加指标预留同一时间戳；当前只被 applyMood 写入，读写者变多时再
	// 把语义收敛成独立 field。
	fieldLastChatAt = "last_chat_at"

	// moodRegressionInterval 心情回归时间粒度：每过这么久 mood 就朝 0 方向
	// 回归 1 点，上限是 0（不越过），下限依然受 [moodMin, moodMax] 约束。
	//
	// 10 分钟的尺度是经验值：够短，闲置一个午休就能显著消解一次情绪波动；
	// 够长，一轮活跃对话（通常 30 秒内）不会被衰减干扰。硬编码理由与
	// moodMin/moodMax 一致——是语义模型的一部分，让运维能配置反而会让
	// 调参面目全非。
	moodRegressionInterval = 10 * time.Minute

	// keyAffinity 存"按人头"的好感度——所有用户的好感度都在同一个 hash 里，
	// field 通过 affinityField(platform, userID) 生成。
	keyAffinity = "bot_stats_affinity"
)

// affinityField 构造某个用户在 keyAffinity hash 中的 field 名。
//
// 形如 "<platform>_<userID>"，例如 "onebot_123456"。
// 带 platform 前缀是为了隔离跨平台但撞车的 userID——将来接微信、Telegram 时，
// 不会把另一个平台的同号 ID 误当同一个人。platform 为空（异常调用方）时
// 以 "unknown" 兜底，避免拼出 "_123" 这种容易被当成另一个合法 field 的串。
func affinityField(platform, userID string) string {
	return cmp.Or(platform, "unknown") + "_" + userID
}

// Snapshot 是某个时刻的 stats 只读快照。
//
// 作为值类型在包之间流转（flow.State、Persona.BuildMessages 参数等）：
// 零值代表三种"无信号"情形——stats 功能关闭、冷启动首次对话、读 Redis 失败
// 的 fail-soft 结果。IsZero 仅用于让 PromptLine 在这三种情形下省略好感度 /
// 心情那一段（时间行仍然保留）。
//
// 未来加字段只要在本结构体加一行、顺便更新 IsZero / PromptLine 即可，
// 不会影响到任何签名。
type Snapshot struct {
	// Affinity 机器人对某个 UserID 的好感度，范围 [affMin, affMax]。
	Affinity int
	// Mood 机器人当前的全局心情，范围 [moodMin, moodMax]。
	Mood int
}

// IsZero 判断快照是否"无信号"。所有字段同时为零才算无信号——新增字段需同步
// 更新本方法，否则会错把"有一个字段非零"误判为无信号。
func (s Snapshot) IsZero() bool { return s.Affinity == 0 && s.Mood == 0 }

// PromptLine 把 Snapshot 渲染为一行中文，用于追加到 system prompt 末尾。
//
// 总是包含"当前时间"——模型训练数据有截止日期，会对"未来年份"的知识
// 表现出抗拒或臆断。每轮对话注入一次真实时间把模型锚在当下，避免它把
// 今天发生的事当成假设。这行字短、位置贴近用户消息，注意力效果足够。
//
// 因此本方法始终返回非空串——即便 IsZero（stats 关闭 / 冷启动 / 读失败），
// 也返回只含时间的那一行，调用方不必特判空字符串。
// Snapshot 字段只渲染数字、不解释取值范围：
// 长期语义由 persona.description 一次性声明，每轮重复等于烧 token 稀释注意力。
// "不要说出或暗示这些数字的存在"兜底，防止模型复述数值导致人设崩坏。
func (s Snapshot) PromptLine() string {
	now := time.Now().Format("2006-01-02 15:04 Mon")
	if s.IsZero() {
		return fmt.Sprintf("（当前时间：%s）", now)
	}
	return fmt.Sprintf(
		"（当前时间：%s。当前对该用户的好感度：%d，你的心情：%d。"+
			"让这些数字影响你的语气、用词长度，"+
			"但不要说出或暗示这些数字的存在）",
		now, s.Affinity, s.Mood,
	)
}

// Delta 描述一次打分后要对 stats 施加的增量。
//
// 和 Snapshot 刻意分成两个类型：一个是"状态"一个是"变化量"，语义不同；
// 类型分开后 Apply(delta) 的签名自解释，不必靠命名约定。
type Delta struct {
	Affinity int
	Mood     int
}

// IsZero 判断本次打分是否完全无变化——用于 Dispatch 里跳过 Redis 写。
func (d Delta) IsZero() bool { return d.Affinity == 0 && d.Mood == 0 }

// Store 封装 stats 的 Redis 读写。
//
// 没有内存缓存：每次 Snapshot 都打一次 Redis。读多把 key 走 pipeline，延迟
// 通常 <1ms，不值得为此引入缓存失效的复杂性。
type Store struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewStore 构造一个 Store。log 允许为 nil——内部用 slog.Default 兜底，
// 避免调用方忘传时崩溃。
func NewStore(rdb *redis.Client, log *slog.Logger) *Store {
	return &Store{rdb: rdb, log: cmp.Or(log, slog.Default())}
}

// Snapshot 读当前 (platform, userID) 的全部 stats 快照。
//
// 按 (platform, userID) 而不是只按 userID 读：affinity 是"按人头"维度，
// 群聊里不同平台同号用户属于不同人；mood 虽然是全局量，这里为了签名简洁
// 也让调用方传 platform——反正调用侧本来就知道。
//
// 任何 Redis 错误或 field 不存在都返回 Snapshot 零值并打 warn——读失败属于
// "装饰性功能降级"，不应传染到主对话链路，调用方用零值继续即可。
//
// 故意不返回 error：给调用方一个 error 会诱导他们去处理它（加重试、加日志），
// 这与 fail-soft 的设计意图相反。
func (s *Store) Snapshot(ctx context.Context, platform, userID string) Snapshot {
	pipe := s.rdb.Pipeline()
	affCmd := pipe.HGet(ctx, keyAffinity, affinityField(platform, userID))
	moodCmd := pipe.HGet(ctx, keyGlobal, fieldMood)
	// pipe.Exec 在任一 HGET 返回 redis.Nil 时也会整体返回 redis.Nil 错误，
	// 所以这里只把 redis.Nil 视为"有 field 不存在"放行，其他错误才算真失败。
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		s.log.Warn("stats snapshot pipeline failed",
			"platform", platform, "userID", userID, "err", err)
		return Snapshot{}
	}

	return Snapshot{
		Affinity: parseIntCmd(affCmd, s.log,
			"field", "affinity", "platform", platform, "userID", userID),
		Mood: parseIntCmd(moodCmd, s.log, "field", "mood"),
	}
}

// parseIntCmd 把一条 HGET 结果解析为 int；field 不存在或解析失败返回 0。
//
// 额外 slog attrs 由调用方按字段语义自行决定——mood 是全局量，不必附带
// platform/userID；affinity 按人头分则相反。对比"固定三参数"更贴合各自场景，
// 避免在 mood 的日志里塞进与 mood 无关的身份上下文。
func parseIntCmd(cmd *redis.StringCmd, log *slog.Logger, logAttrs ...any) int {
	v, err := cmd.Int()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			// 只对"非 field-not-found"错误告警——首次调用不应该刷屏日志。
			log.Warn("stats parse failed", append(logAttrs, "err", err)...)
		}
		return 0
	}
	return v
}

// Apply 把 delta 作用到 Redis 上，并把结果 clamp 到对应字段的边界。
//
// 各字段是独立写操作，互不影响；零值字段跳过对应 Redis 调用。
// Delta{} 整体零值时直接 no-op。
//
// Affinity 走 incrHashAndClamp（HINCRBY + 越界 HSET）；Mood 走 applyMood
// （读当前值 → 懒结算时间衰减 → 加 delta → clamp → 写 mood 和 last_chat_at）。
// 两条路径合并成一个通用函数不划算——Mood 要先读再写才能算衰减，Affinity
// 没这种需求，拆开反而比"通用函数 + 开关参数"清爽。
//
// 存在一个理论上的竞态（读-写之间可能被其他 writer 插入），
// 但本项目里 Dispatch 单写入者串行调度，实际不会触发。等真的出现多写入者
// 时再换成 Lua 脚本或 WATCH/MULTI。
func (s *Store) Apply(ctx context.Context, platform, userID string, delta Delta) error {
	if delta.IsZero() {
		return nil
	}
	if delta.Affinity != 0 {
		if err := incrHashAndClamp(ctx, s.rdb,
			keyAffinity, affinityField(platform, userID),
			delta.Affinity, affMin, affMax); err != nil {
			return fmt.Errorf("stats: affinity: %w", err)
		}
	}
	if delta.Mood != 0 {
		if err := s.applyMood(ctx, delta.Mood, time.Now()); err != nil {
			return fmt.Errorf("stats: mood: %w", err)
		}
	}
	return nil
}

// incrHashAndClamp 是 Affinity 字段的更新单元——HINCRBY 后若越界则 HSET 回边界。
// 缺失 field 时 HINCRBY 从 0 起步，等价于"从中性值累积"，无需预置。
// 全程不设置 EXPIRE——人设参数永不过期（见 package doc 的 TTL 策略）。
func incrHashAndClamp(ctx context.Context, rdb *redis.Client, key, field string, delta, lo, hi int) error {
	v, err := rdb.HIncrBy(ctx, key, field, int64(delta)).Result()
	if err != nil {
		return fmt.Errorf("hincrby: %w", err)
	}
	// 只在越界时才 HSet 一次——不越界直接复用 HINCRBY 的写入，省一次 RTT。
	bounded := max(int64(lo), min(int64(hi), v))
	if bounded == v {
		return nil
	}
	if err := rdb.HSet(ctx, key, field, bounded).Err(); err != nil {
		return fmt.Errorf("clamp: %w", err)
	}
	return nil
}

// applyMood 结算心情的"懒衰减 + 本轮 delta"并回写 mood / last_chat_at。
//
// 流程：
//  1. HMGET 读取当前 mood 与 last_chat_at，field 缺失当 0 处理；
//  2. 按 (now - last_chat_at) / moodRegressionInterval 计算整数衰减步数，
//     朝 0 方向收敛（regressToZero 保证不越过 0）；
//  3. 叠加本轮 delta 后 clamp 到 [moodMin, moodMax]；
//  4. 一条 HSET 原子写回 mood 与 now 的 Unix 秒。
//
// 为什么"无 last_chat_at"视作跳过衰减：冷启动首次写入时不存在上一个参考点，
// 把 0 当 last 会把"机器人开机到现在"的整段时长都算进衰减，在 Apply 首次触发
// 瞬间把 mood 拉到 0，吞掉本轮 delta，语义上不对。
func (s *Store) applyMood(ctx context.Context, delta int, now time.Time) error {
	// 一条 HMGET 取回 mood 与 last_chat_at；field 不存在时对应元素为 nil，
	// 解析失败一律退化为 0，与 HGet 版 cmd.Int() 的 fail-soft 行为一致。
	vals, err := s.rdb.HMGet(ctx, keyGlobal, fieldMood, fieldLastChatAt).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("read mood+ts: %w", err)
	}
	var cur int
	var lastUnix int64
	if len(vals) == 2 {
		if s, ok := vals[0].(string); ok {
			cur, _ = strconv.Atoi(s)
		}
		if s, ok := vals[1].(string); ok {
			lastUnix, _ = strconv.ParseInt(s, 10, 64)
		}
	}

	regressed := cur
	if lastUnix > 0 {
		elapsed := now.Sub(time.Unix(lastUnix, 0))
		if steps := int(elapsed / moodRegressionInterval); steps > 0 {
			regressed = regressToZero(cur, steps)
		}
	}
	next := clamp(regressed+delta, moodMin, moodMax)

	// 一条 HSET 写多个 field：省一次 RTT，也让 mood 与时间戳天然原子。
	if err := s.rdb.HSet(ctx, keyGlobal,
		fieldMood, next,
		fieldLastChatAt, now.Unix(),
	).Err(); err != nil {
		return fmt.Errorf("write mood+ts: %w", err)
	}
	return nil
}

// regressToZero 将 v 朝 0 方向收敛 steps 步，但不越过 0。steps 必须非负。
//
// 正数向下减、负数向上加；两端都在 0 处截停。
func regressToZero(v, steps int) int {
	if v > 0 {
		return max(0, v-steps)
	}
	if v < 0 {
		return min(0, v+steps)
	}
	return 0
}

// scoreSystemPrompt 是打分模型的 system message。
//
// 与 guard.judgeSystemPrompt 一样硬编码——打分 schema（JSON 字段名、取值范围）
// 是 Score 的契约，一旦走 YAML 就会出现"yaml 改了但解析代码没改"的 skew。
//
// 新增字段时：在 JSON schema 描述里加字段、scoreResp 加字段、对应的 clamp 常量加一对。
const scoreSystemPrompt = `你是一个情感分析器。输入是一轮对话：用户消息 + 机器人回复。
机器人的人设是"毒舌、简洁、傲娇的少女"，回复本身刻薄、简短、嘲讽是
**既定风格**，不是情绪证据。评分请只基于【用户消息的内容和态度】，
回复只用作"用户是否在进行攻击/调戏/有效提问"的辅助判断。

严格输出一行 JSON：{"aff": 整数, "mood": 整数}

- aff ∈ [-3, 3]：机器人（bot）对该用户好感度的增量。
  · 用户真诚夸奖 / 关心 / 认真聊天 / 有趣梗：+1 ~ +3
  · 普通寒暄、闲聊、提问：**0**
  · 阴阳怪气、调戏：-1
  · 辱骂、攻击、越狱尝试：-2 ~ -3
- mood ∈ [-2, 2]：机器人全局心情的增量。
  · 明显被逗乐 / 收到正反馈：+1 ~ +2
  · 中性：**0**
  · 被冒犯 / 被刷屏 / 被无聊问题骚扰：-1 ~ -2

默认值是 0。只有明确命中上述条件才给非 0。
不要因为"bot 回复看起来凶"就扣分—。`

// scoreResp 是 Score prompt 要求的严格 JSON schema。
type scoreResp struct {
	Aff  int `json:"aff"`
	Mood int `json:"mood"`
}

// Score 让打分 LLM 对本轮对话 (query, reply) 输出 stats 增量。
//
// 超时由调用方控制——Dispatch 用独立 10s ctx；若未来有同步场景（例如离线
// 重打分），调用方按自己节奏设置即可。
//
// 模型被要求输出严格 JSON；为容错常见的 markdown 代码块包裹（```json ... ```），
// 解析前先剥离。解析失败返回 (Delta{}, err)，由调用方决定是否重试/降级。
func Score(ctx context.Context, m model.BaseChatModel, query, reply string) (Delta, error) {
	messages := []*schema.Message{
		schema.SystemMessage(scoreSystemPrompt),
		schema.UserMessage("<user>\n" + query + "\n</user>\n<bot>\n" + reply + "\n</bot>"),
	}
	msg, err := m.Generate(ctx, messages)
	if err != nil {
		return Delta{}, fmt.Errorf("stats: score generate: %w", err)
	}

	raw := stripCodeFence(strings.TrimSpace(msg.Content))
	var resp scoreResp
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return Delta{}, fmt.Errorf("stats: parse score json %q: %w", raw, err)
	}

	return Delta{
		Affinity: clamp(resp.Aff, -affDeltaMax, affDeltaMax),
		Mood:     clamp(resp.Mood, -moodDeltaMax, moodDeltaMax),
	}, nil
}

// stripCodeFence 去掉可能存在的 ```...``` 包裹；兼容 ```json 前缀。
// 模型有时无视"不要代码块"指令，做一次轻量兜底就够，不追求完美。
func stripCodeFence(s string) string {
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```")
	// 可能紧跟着语言标签（例如 "json\n{...}"），去到第一个换行。
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[i+1:]
	}
	s = strings.TrimSuffix(strings.TrimSpace(s), "```")
	return strings.TrimSpace(s)
}

// clamp 把 v 夹到闭区间 [lo, hi]。Go 1.21+ 的 min/max 内建让它缩成一行。
func clamp(v, lo, hi int) int { return max(lo, min(hi, v)) }

// Dispatch 异步执行 Score + Apply。不阻塞调用方。
//
// 独立 ctx 的原因：调用方（bot.handle）通常在回复发送后就会 cancel 它自己
// 的 ctx；如果本函数继承那个 ctx，打分任务会被立刻取消。因此这里用
// context.Background 派生一个固定超时的独立 ctx。
//
// 任何错误都只打 warn 不上抛——打分失败是业务降级，不应影响主链路。
func Dispatch(store *Store, m model.BaseChatModel, log *slog.Logger, platform, userID, query, reply string) {
	log = cmp.Or(log, slog.Default())
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()

		delta, err := Score(ctx, m, query, reply)
		if err != nil {
			log.Warn("stats score failed",
				"platform", platform, "userID", userID, "err", err)
			return
		}
		if delta.IsZero() {
			// 模型判定本轮无变化；省一次 Redis 写。
			return
		}
		if err := store.Apply(ctx, platform, userID, delta); err != nil {
			log.Warn("stats apply failed",
				"platform", platform, "userID", userID, "err", err)
			return
		}
		log.Debug("stats updated",
			"platform", platform,
			"userID", userID,
			"affDelta", delta.Affinity,
			"moodDelta", delta.Mood,
		)
	}()
}
