// Package stats 管理机器人运行期会影响回复风格的"人设参数"。
//
// 当前包含两个参数：
//   - Affinity（好感度）：按"平台 + 用户"分 member，存在全局 ZSET
//     "bot_stats_affinity_rank" 里，member 形如 "<platform>_<userID>"。
//     语义是"某个用户在机器人心里的累计好感度"，按人头维护，仅供 passive
//     主链使用（按 member 点查当前用户分值）。选 ZSET 而不是 Hash，是为了
//     未来扩展按 score 范围扫描时不必再迁库；当前 proactive 链路不再读取它。
//   - Mood（心情）：全局 Hash "bot_stats_global"，field "mood"，
//     语义是"机器人当前整体心情"，所有用户共享——人不会因为换一个聊天对象
//     就切换心情。放在 hash 里而不是独立 string key，是为了给后续扩展的
//     "其他全局变量"（疲劳度、在线时长、连续对话计数……）留好位置，
//     每增加一个全局量就是 hash 里多一个 field，不用再开新的 key。
//
// TTL 策略：stats key 都永不过期——人设是长期累积的结果，
// 用户"沉寂 30 天再回来"时理应记得他们之前惹没惹过机器人。与 history
// key 的滑动过期策略（见 store.history.go）刻意相反。
//
// 心情的"自然衰减"：人情绪上头总会冷静回来，同一个模型应体现在 mood 上。
// 实现方式是让 mood 每过 moodRegressionInterval（30 分钟）朝 moodBaseline（15）
// 回归 1 点——比 0 偏正是因为"闲下来后逐渐缓过来"比"绝对中性"更贴合人设，
// 空闲时段回来的新对话不至于每次都从冷冰冰的 0 起步。收敛时不越过 baseline，
// 即正向回落不会穿到负面、反之亦然。衰减不用定时任务，而是在 Store.Snapshot
// 读取时顺手懒结算：
//   - 省掉一个独立的后台 goroutine / 定时器；
//   - 不活跃时段不产生任何 Redis 流量；
//   - 一次 pipeline 同时拿到读所需值并按需写回回归后的 mood，避免长时间空闲后
//     的第一轮回复仍带着旧情绪。
//
// 为此 keyGlobal 里额外存一个 fieldLastChatAt（Unix 秒），记录上次 mood 结算 /
// 写入的时刻；在 Snapshot 或 applyMood 结算完成后顺带刷新。
//
// 两个参数都是整数，0 代表中性，正负对称。Redis 缺 affinity member 时从 0
// 起步；缺 mood field 时从 moodInitial 起步，匹配默认回归心情。
//
// 扩展参数（疲劳度 / 信任度 / 饱腹感 ...）时，改动集中在本文件：
//  1. 在 Snapshot / Delta 结构体里加字段；
//  2. 在 Store.Snapshot / Store.Apply 里把字段加到对应存储（全局量进
//     keyGlobal、按人量进 keyAffinity 那样的独立 rank 或同一 rank 的新 member）；
//  3. 在 stats 打分 prompt 和 scoreResp 里加对应的 JSON 字段；
//  4. 在 Snapshot.PromptLine 里把新字段渲染成面向 prompt 的短标签。
//
// 其他地方（flow.State / Persona.BuildMessages / nodes / bot / main）对存储与
// 回归策略不感知——它们只搬运本轮快照里的标量值。
//
// 设计约束：
//  1. 无 L2 内存缓存：Redis 是唯一真相源，避免多实例部署时的缓存一致性问题；
//  2. 软降级：读写任何失败都不应阻断主对话——读失败返回零值继续，
//     写失败只打日志不上抛；
//  3. 打分链路完全异步：打分与写入由 Dispatch 起独立 goroutine 执行，
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
	"github.com/echo/llm-bot/internal/llmtext"
	"github.com/redis/go-redis/v9"
)

// 硬编码的边界与常量。
//
// 边界是 stats 语义模型的一部分（与打分 prompt 里声明的 delta 范围一一对应），
// 允许运维在 prompt 或配置里改动会让打分契约与存储边界脱节，因此刻意不做配置化。
const (
	// Affinity 范围：0 为中性，正为喜欢，负为讨厌。
	affMin, affMax = -100, 100

	// Mood 范围：0 为平静，正为愉悦，负为烦躁。
	moodMin, moodMax = -50, 50

	// moodInitial 是 Redis 缺 mood field 时使用的启动值。
	moodInitial = 15

	// moodBaseline 是心情的自然回归目标——空闲时 mood 会朝这里收敛而不是 0。
	// 选 15 是为了让空闲后的状态回到"放松偏好"，
	// 贴合"傲娇但内心其实不排斥聊天"的人设；落到 0 的中性值反而显得偏冷。
	// 必须落在 [moodMin, moodMax] 之内，否则 applyMood 的 clamp 会把它再夹回来。
	moodBaseline = 15

	// 单轮打分的 delta 上限，与打分提示词里声明的范围保持一致；
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
	// fieldLastChatAt 是 keyGlobal 中保存"上次心情结算 / 写入时刻"的 field。
	// 存 Unix 秒而非 RFC3339 字符串：规避时区 / 闰秒的解析坑，整型也便于
	// Redis 内部比较和未来做 ZADD 之类的时间窗操作。
	//
	// 命名上留"chat_at"而非"mood_at"，是为了给将来"会话计数 / 活跃时段"
	// 等附加指标预留同一时间戳；读写者继续变多时再
	// 把语义收敛成独立 field。
	fieldLastChatAt = "last_chat_at"

	// moodRegressionInterval 心情回归时间粒度：每过这么久 mood 就朝
	// moodBaseline 方向回归 1 点，不越过 baseline，仍受 [moodMin, moodMax] 约束。
	//
	// 30 分钟的尺度是经验值：够短，闲置半个多小时就能显著消解一次情绪波动；
	// 够长，一轮活跃对话（通常 30 秒内）不会被衰减干扰。硬编码理由与
	// moodMin/moodMax 一致——是语义模型的一部分，让运维能配置反而会让
	// 调参面目全非。
	moodRegressionInterval = 30 * time.Minute

	// keyAffinity 存"按人头"的好感度排行——所有用户的好感度都在同一个 ZSET 里，
	// member 通过 affinityField(platform, userID) 生成，score 就是当前好感度。
	// 这份 ZSET 仅供 passive 主链消费（按 member 点查当前用户分值）；
	// 选用 ZSET 而不是 Hash 是为了未来扩展按 score 范围扫描时不必再迁库。
	keyAffinity = "bot_stats_affinity_rank"
)

// affinityField 构造某个用户在 keyAffinity ZSET 中的 member 名。
//
// 形如 "<platform>_<userID>"，例如 "onebot_123456"。
// 带 platform 前缀是为了隔离跨平台但撞车的 userID——将来接微信、Telegram 时，
// 不会把另一个平台的同号 ID 误当同一个人。platform 为空（异常调用方）时
// 以 "unknown" 兜底，避免拼出 "_123" 这种容易被当成另一个合法 member 的串。
func affinityField(platform, userID string) string {
	return cmp.Or(platform, "unknown") + "_" + userID
}

// Snapshot 是某个时刻的 stats 只读快照。
//
// 作为值类型在包之间流转（flow.State、Persona.BuildMessages 参数等）：
// 零值代表两种"无信号"情形——stats 功能关闭、读 Redis 失败的软降级结果。
// IsZero 仅用于让 PromptLine 在这两种情形下省略好感度 /
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

// PromptLine 把 Snapshot 渲染为一行中文，用于追加到系统提示词末尾。
//
// 总是包含"当前时间"——模型训练数据有截止日期，会对"未来年份"的知识
// 表现出抗拒或臆断。每轮对话注入一次真实时间把模型锚在当下，避免它把
// 今天发生的事当成假设。这行字短、位置贴近用户消息，注意力效果足够。
//
// 因此本方法始终返回非空串——即便 IsZero（stats 关闭 / 读失败），
// 也返回只含时间的那一行，调用方不必特判空字符串。
// Snapshot 字段渲染为离散短标签，而不是把原始数字直接暴露给主模型：
// 关系 / 心情的长期语义留在代码阈值里，persona 只需要知道"状态会影响语气"。
// "不要说出或暗示这些状态的存在"兜底，防止模型复述系统状态导致人设崩坏。
func (s Snapshot) PromptLine() string {
	now := time.Now().Format("2006-01-02 15:04 Mon")
	if s.IsZero() {
		return fmt.Sprintf("（当前时间：%s）", now)
	}
	return fmt.Sprintf(
		"（当前时间：%s。你和这个用户：%s；你现在：%s。"+
			"让这些状态影响你的语气和回复长度，"+
			"但不要说出或暗示这些状态的存在）",
		now, affinityPromptLabel(s.Affinity), moodPromptLabel(s.Mood),
	)
}

// affinityPromptLabel 把内部好感度分值压成面向 prompt 的短关系标签。
// 阈值由旧七档合并而来：保留主要语气差异，避免把长表塞进 system prompt。
func affinityPromptLabel(v int) string {
	v = clamp(v, affMin, affMax)
	switch {
	case v <= -40:
		return "很烦"
	case v <= -11:
		return "不熟"
	case v <= 10:
		return "普通"
	case v <= 74:
		return "熟了"
	default:
		return "很亲近"
	}
}

// moodPromptLabel 把内部心情分值压成面向 prompt 的短心情标签。
// moodBaseline=15 会落在"心情好"，对应空闲后逐渐缓过来的默认状态。
func moodPromptLabel(v int) string {
	v = clamp(v, moodMin, moodMax)
	switch {
	case v <= -20:
		return "很烦"
	case v <= -6:
		return "有点烦"
	case v <= 8:
		return "普通"
	case v <= 22:
		return "心情好"
	default:
		return "很开心"
	}
}

// Delta 描述一次打分后要对 stats 施加的增量。
//
// 和 Snapshot 刻意分成两个类型：一个是"状态"一个是"变化量"，语义不同；
// 类型分开后 Apply(delta) 的签名自解释，不必靠命名约定。
type Delta struct {
	// Affinity 是本轮对用户好感度的增量，Apply 时会夹到单轮与总量边界内。
	Affinity int
	// Mood 是本轮对全局心情的增量，Apply 前会先结算自然回归。
	Mood int
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

// Snapshot 读当前 (platform, userID) 的全部 stats 快照，并顺带懒结算 mood 自然回归。
//
// 按 (platform, userID) 而不是只按 userID 读：affinity 是"按人头"维度，
// 群聊里不同平台同号用户属于不同人；mood 虽然是全局量，这里为了签名简洁
// 也让调用方传 platform——反正调用侧本来就知道。
//
// 一次 pipeline 同时取 affinity / mood / last_chat_at；如果 (now - last_chat_at)
// 跨过 moodRegressionInterval，把回归后的 mood 与 now 写回 Redis，并使用回归值
// 注入本轮 prompt。这样把"按时间自然冷却"挪到真实对话到来时懒结算，避免为了
// 装饰性信号常驻定时器。
//
// Redis 读错误返回 Snapshot 零值并打 warn；affinity member 不存在时按 0 处理，
// mood field 不存在时按 moodInitial 处理。回写失败只 warn，使用未回归值，
// 避免把装饰性 stats 故障传播到对话生成。
//
// 故意不返回 error：给调用方一个 error 会诱导他们去处理它（加重试、加日志），
// 这与软降级的设计意图相反。
func (s *Store) Snapshot(ctx context.Context, platform, userID string, now time.Time) Snapshot {
	pipe := s.rdb.Pipeline()
	affCmd := pipe.ZScore(ctx, keyAffinity, affinityField(platform, userID))
	moodCmd := pipe.HMGet(ctx, keyGlobal, fieldMood, fieldLastChatAt)
	// pipe.Exec 在任一 ZSCORE/HGET 返回 redis.Nil 时也会整体返回 redis.Nil 错误，
	// 所以这里只把 redis.Nil 视为"有 member / field 不存在"放行，其他错误才算真失败。
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		s.log.Warn("stats snapshot pipeline failed",
			"platform", platform, "userID", userID, "err", err)
		return Snapshot{}
	}

	aff := parseZScoreCmd(affCmd, s.log,
		"member", "affinity", "platform", platform, "userID", userID)
	cur, lastUnix := parseMoodState(moodCmd.Val())
	mood := cur
	if regressed := regressMood(cur, lastUnix, now); regressed != cur {
		if err := s.rdb.HSet(ctx, keyGlobal,
			fieldMood, regressed,
			fieldLastChatAt, now.Unix(),
		).Err(); err != nil {
			s.log.Warn("stats mood regression failed", "err", err)
		} else {
			mood = regressed
		}
	}

	return Snapshot{Affinity: aff, Mood: mood}
}

// parseMoodState 解析 HMGET mood/last_chat_at 的结果；缺失 mood 按 moodInitial 处理。
func parseMoodState(vals []any) (cur int, lastUnix int64) {
	cur = moodInitial
	if len(vals) != 2 {
		return cur, 0
	}
	if raw, ok := vals[0].(string); ok {
		if parsed, err := strconv.Atoi(raw); err == nil {
			cur = parsed
		}
	}
	if raw, ok := vals[1].(string); ok {
		lastUnix, _ = strconv.ParseInt(raw, 10, 64)
	}
	return cur, lastUnix
}

// parseZScoreCmd 把一条 ZSCORE 结果解析为 int；member 不存在或解析失败返回 0。
func parseZScoreCmd(cmd *redis.FloatCmd, log *slog.Logger, logAttrs ...any) int {
	v, err := cmd.Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Warn("stats parse failed", append(logAttrs, "err", err)...)
		}
		return 0
	}
	return int(v)
}

// Apply 把 delta 作用到 Redis 上，并把结果 clamp 到对应字段的边界。
//
// 各字段是独立写操作，互不影响；零值字段跳过对应 Redis 调用。
// Delta{} 整体零值时直接空操作。
//
// Affinity 走 incrZSetAndClamp（ZINCRBY + 越界 ZADD）；Mood 走 applyMood
// （兜底懒结算时间衰减 → 加 delta → clamp → 写 mood 和 last_chat_at）。
// 两条路径合并成一个通用函数不划算——Mood 要先读再写才能算衰减，Affinity
// 没这种需求，拆开反而比"通用函数 + 开关参数"清爽。
//
// clamp 存在并发窗口：ZINCRBY 与越界后的 ZADD 之间可能插入其他写入。
// 当前 stats 是影响语气的装饰性信号，短暂越界会在后续写入再次夹回；如果未来
// 好感度排行变成强一致需求，再把这段替换为 Lua 脚本或 WATCH/MULTI。
func (s *Store) Apply(ctx context.Context, platform, userID string, delta Delta) error {
	if delta.IsZero() {
		return nil
	}
	if delta.Affinity != 0 {
		if err := incrZSetAndClamp(ctx, s.rdb,
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

// incrZSetAndClamp 是 Affinity member 的更新单元——ZINCRBY 后若越界则 ZADD 回边界。
//
// 选择 ZSET 而不是 Hash，是因为好感度既要按用户点查，也要按 score 排序给主动
// 消息候选使用；ZSET 的 score 正好同时覆盖这两个需求。Redis 没有"有界自增"
// 原语，所以这里用 ZINCRBY 先完成增量，再在越界时补一次 ZADD 拉回边界。
// 缺失 member 时 ZINCRBY 从 0 起步，等价于"从中性值累积"，无需预置。
// 全程不设置 EXPIRE——人设参数永不过期（见 package doc 的 TTL 策略）。
func incrZSetAndClamp(ctx context.Context, rdb *redis.Client, key, member string, delta, lo, hi int) error {
	v, err := rdb.ZIncrBy(ctx, key, float64(delta), member).Result()
	if err != nil {
		return fmt.Errorf("zincrby: %w", err)
	}
	// 只在越界时才 ZAdd 一次——不越界直接复用 ZINCRBY 的写入，省一次 RTT。
	bounded := float64(clamp(int(v), lo, hi))
	if bounded == v {
		return nil
	}
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: bounded, Member: member}).Err(); err != nil {
		return fmt.Errorf("clamp: %w", err)
	}
	return nil
}

// applyMood 结算心情的"本轮 delta"并回写 mood / last_chat_at。
//
// 正常主链路会在生成回复前由 Snapshot 先结算自然回归；这里仍保留回归计算，
// 作为直接调用 Apply、Snapshot 写回失败或非标准调用顺序的兜底。
//
// 流程：
//  1. HMGET 读取当前 mood 与 last_chat_at，mood field 缺失当 moodInitial 处理；
//  2. 按 (now - last_chat_at) / moodRegressionInterval 计算整数衰减步数，
//     朝 moodBaseline 方向收敛（regressMood 保证不越过 baseline）；
//  3. 叠加本轮 delta 后 clamp 到 [moodMin, moodMax]；
//  4. 一条 HSET 原子写回 mood 与 now 的 Unix 秒。
//
// 为什么"无 last_chat_at"视作跳过衰减：冷启动首次写入时不存在上一个参考点，
// 把 0 当 last 会把"机器人开机到现在"的整段时长都算进衰减，在 Apply 首次触发
// 瞬间把 mood 拉到 0，吞掉本轮 delta，语义上不对。
func (s *Store) applyMood(ctx context.Context, delta int, now time.Time) error {
	// 一条 HMGET 取回 mood 与 last_chat_at；mood field 缺失时按 moodInitial 起步。
	vals, err := s.rdb.HMGet(ctx, keyGlobal, fieldMood, fieldLastChatAt).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("read mood+ts: %w", err)
	}
	cur, lastUnix := parseMoodState(vals)
	regressed := regressMood(cur, lastUnix, now)
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

// regressMood 根据 last_chat_at 计算 cur 在 now 时刻自然回归后的值。
//
// cur 高于 baseline 时向下减、低于 baseline 时向上加；两端都在 baseline 处截停。
func regressMood(cur int, lastUnix int64, now time.Time) int {
	if lastUnix <= 0 {
		return cur
	}
	elapsed := now.Sub(time.Unix(lastUnix, 0))
	steps := int(elapsed / moodRegressionInterval)
	if steps <= 0 {
		return cur
	}
	if cur > moodBaseline {
		return max(moodBaseline, cur-steps)
	}
	if cur < moodBaseline {
		return min(moodBaseline, cur+steps)
	}
	return moodBaseline
}

// LoadScorePrompt 读取 stats 打分模型的 system prompt。
func LoadScorePrompt(path string) (string, error) {
	return llmtext.LoadPromptFile(path, "stats")
}

// scoreResp 是打分提示词要求的严格 JSON 结构。
type scoreResp struct {
	Aff  int `json:"aff"`
	Mood int `json:"mood"`
}

// Score 让打分 LLM 对本轮对话 (query, reply) 输出 stats 增量。
//
// 超时由调用方控制——Dispatch 用独立 10s ctx；若未来有同步场景（例如离线
// 重打分），调用方按自己节奏设置即可。
//
// 模型被要求输出严格 JSON；为容错常见的 Markdown 代码块包裹（```json ... ```），
// 解析前先剥离。解析失败返回 (Delta{}, err)，由调用方决定是否重试/降级。
func Score(ctx context.Context, m model.BaseChatModel, systemPrompt, query, reply string) (Delta, error) {
	messages := []*schema.Message{
		schema.SystemMessage(systemPrompt),
		schema.UserMessage("<user>\n" + query + "\n</user>\n<bot>\n" + reply + "\n</bot>"),
	}
	msg, err := m.Generate(ctx, messages)
	if err != nil {
		return Delta{}, fmt.Errorf("stats: score generate: %w", err)
	}

	raw := llmtext.StripCodeFence(strings.TrimSpace(msg.Content))
	var resp scoreResp
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return Delta{}, fmt.Errorf("stats: parse score json %q: %w", raw, err)
	}

	return Delta{
		Affinity: clamp(resp.Aff, -affDeltaMax, affDeltaMax),
		Mood:     clamp(resp.Mood, -moodDeltaMax, moodDeltaMax),
	}, nil
}

// clamp 把 v 夹到闭区间 [lo, hi]。Go 1.21+ 的 min/max 内建让它缩成一行。
func clamp(v, lo, hi int) int { return max(lo, min(hi, v)) }

// Dispatch 异步执行打分与写入，不阻塞调用方。
//
// 独立 ctx 的原因：调用方（bot.handle）通常在回复发送后就会 cancel 它自己
// 的 ctx；如果本函数继承那个 ctx，打分任务会被立刻取消。因此这里用
// context.Background 派生一个固定超时的独立 ctx。
//
// 任何错误都只打 warn 不上抛——打分失败是业务降级，不应影响主链路。
func Dispatch(store *Store, m model.BaseChatModel, scorePrompt string, log *slog.Logger, platform, userID, query, reply string) {
	log = cmp.Or(log, slog.Default())
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()

		delta, err := Score(ctx, m, scorePrompt, query, reply)
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
