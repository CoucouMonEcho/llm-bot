// Package stats 管理机器人运行期会影响回复风格的"人设参数"。
//
// 当前包含两个参数：
//   - Affinity（好感度）：按 userID 分 key，形如 "bot:stats:aff:<userID>"，
//     语义是"某个用户在机器人心里的累计好感度"；
//   - Mood（心情）：全局单 key "bot:stats:mood"，语义是"机器人当前整体心情"，
//     所有用户共享——人不会因为换一个聊天对象就切换心情。
//
// 两个参数都是整数，0 代表中性，正负对称。Redis 缺 key 时 INCRBY 从 0 起步，
// 恰好等价于"从中性开始累积"，不需要任何预初始化。
//
// 扩展参数（疲劳度 / 信任度 / 饱腹感 ...）时，改动集中在本文件：
//  1. 在 Snapshot / Delta 结构体里加字段；
//  2. 在 Store.Snapshot / Store.Apply 里加一把 key 的读写；
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

	keyMood = "bot:stats:mood"
)

// keyAffinity 构造某个用户的好感度 key。
// 单独抽函数是为了跟 keyMood 对齐，并便于将来加前缀或迁移。
func keyAffinity(userID string) string { return "bot:stats:aff:" + userID }

// Snapshot 是某个时刻的 stats 只读快照。
//
// 作为值类型在包之间流转（flow.State、Persona.BuildMessages 参数等）：
// 零值同时代表三种"无信号"情形，上游统一按 IsZero / PromptLine 跳过追加
// 状态行的逻辑：
//   - stats 功能关闭；
//   - 冷启动、用户首次对话；
//   - 读 Redis 失败的 fail-soft 结果。
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
// IsZero 时返回空串——让调用方按"空串=不追加"统一处理。
//
// 只渲染数字、不解释取值范围与方向：后者属于长期设定，应由 persona.description
// 一次性声明；每轮都重复等于烧 token，还会稀释模型对 persona 本体的注意力。
// 措辞保留"不要说出或暗示这些数字的存在"兜底，防止模型复述数值导致人设崩坏。
func (s Snapshot) PromptLine() string {
	if s.IsZero() {
		return ""
	}
	return fmt.Sprintf(
		"（当前对该用户的好感度：%d，你的心情：%d。"+
			"让这些数字影响你的语气、用词长度、是否主动关心，"+
			"但不要说出或暗示这些数字的存在。）",
		s.Affinity, s.Mood,
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
	if log == nil {
		log = slog.Default()
	}
	return &Store{rdb: rdb, log: log}
}

// Snapshot 读当前 userID 的全部 stats 快照。
//
// 任何 Redis 错误或 key 不存在都返回 Snapshot 零值并打 warn——读失败属于
// "装饰性功能降级"，不应传染到主对话链路，调用方用零值继续即可。
//
// 故意不返回 error：给调用方一个 error 会诱导他们去处理它（加重试、加日志），
// 这与 fail-soft 的设计意图相反。
func (s *Store) Snapshot(ctx context.Context, userID string) Snapshot {
	pipe := s.rdb.Pipeline()
	affCmd := pipe.Get(ctx, keyAffinity(userID))
	moodCmd := pipe.Get(ctx, keyMood)
	// pipe.Exec 在任一 GET 返回 redis.Nil 时也会整体返回 redis.Nil 错误，
	// 所以这里只把 redis.Nil 视为"有 key 不存在"放行，其他错误才算真失败。
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		s.log.Warn("stats snapshot pipeline failed", "userID", userID, "err", err)
		return Snapshot{}
	}

	return Snapshot{
		Affinity: parseIntCmd(affCmd, s.log, "affinity", userID),
		Mood:     parseIntCmd(moodCmd, s.log, "mood", userID),
	}
}

// parseIntCmd 把一条 GET 结果解析为 int；key 不存在或解析失败返回 0。
func parseIntCmd(cmd *redis.StringCmd, log *slog.Logger, field, userID string) int {
	v, err := cmd.Int()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			// 只对"非 key-not-found"错误告警——首次调用不应该刷屏日志。
			log.Warn("stats parse failed", "field", field, "userID", userID, "err", err)
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
// 存在一个理论上的竞态（INCRBY 和越界 SET 回拉之间可能被其他 writer 插入），
// 但本项目里 Dispatch 单写入者串行调度，实际不会触发。等真的出现多写入者
// 时再换成 Lua 脚本或 WATCH/MULTI。
func (s *Store) Apply(ctx context.Context, userID string, delta Delta) error {
	if delta.IsZero() {
		return nil
	}
	if delta.Affinity != 0 {
		if err := incrAndClamp(ctx, s.rdb, keyAffinity(userID), delta.Affinity, affMin, affMax); err != nil {
			return fmt.Errorf("stats: affinity: %w", err)
		}
	}
	if delta.Mood != 0 {
		if err := incrAndClamp(ctx, s.rdb, keyMood, delta.Mood, moodMin, moodMax); err != nil {
			return fmt.Errorf("stats: mood: %w", err)
		}
	}
	return nil
}

// incrAndClamp 是 Apply 的字段级更新单元——INCRBY 后若越界则 SET 回边界。
// 缺失 key 时 INCRBY 从 0 起步，等价于"从中性值累积"，无需预置。
func incrAndClamp(ctx context.Context, rdb *redis.Client, key string, delta, lo, hi int) error {
	v, err := rdb.IncrBy(ctx, key, int64(delta)).Result()
	if err != nil {
		return fmt.Errorf("incrby: %w", err)
	}
	switch {
	case v > int64(hi):
		if err := rdb.Set(ctx, key, hi, 0).Err(); err != nil {
			return fmt.Errorf("clamp max: %w", err)
		}
	case v < int64(lo):
		if err := rdb.Set(ctx, key, lo, 0).Err(); err != nil {
			return fmt.Errorf("clamp min: %w", err)
		}
	}
	return nil
}

// scoreSystemPrompt 是打分模型的 system message。
//
// 与 guard.judgeSystemPrompt 一样硬编码——打分 schema（JSON 字段名、取值范围）
// 是 Score 的契约，一旦走 YAML 就会出现"yaml 改了但解析代码没改"的 skew。
//
// 新增字段时：在 JSON schema 描述里加字段、scoreResp 加字段、对应的 clamp 常量加一对。
const scoreSystemPrompt = `你是一个情感分析器。输入包含一段用户消息和机器人回复。请判断这轮对话对机器人的影响。

严格只输出一行 JSON，格式如下，不要任何解释、不要代码块包裹：
{"aff": 整数, "mood": 整数}

- aff 取 [-3, 3]：用户对机器人的好感度增量。
  正面/关心/夸奖/有趣 → 正值；辱骂/攻击/冷漠 → 负值；中性日常 → 0。
- mood 取 [-2, 2]：机器人当前心情的增量。
  轻松愉快/被逗乐 → 正值；烦躁/被冒犯/无聊 → 负值；中性 → 0。

只回答 JSON，绝不添加解释、标点、前后缀。`

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

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// Dispatch 异步执行 Score + Apply。不阻塞调用方。
//
// 独立 ctx 的原因：调用方（bot.handle）通常在回复发送后就会 cancel 它自己
// 的 ctx；如果本函数继承那个 ctx，打分任务会被立刻取消。因此这里用
// context.Background 派生一个固定超时的独立 ctx。
//
// 任何错误都只打 warn 不上抛——打分失败是业务降级，不应影响主链路。
func Dispatch(store *Store, m model.BaseChatModel, log *slog.Logger, userID, query, reply string) {
	if log == nil {
		log = slog.Default()
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()

		delta, err := Score(ctx, m, query, reply)
		if err != nil {
			log.Warn("stats score failed", "userID", userID, "err", err)
			return
		}
		if delta.IsZero() {
			// 模型判定本轮无变化；省一次 Redis 写。
			return
		}
		if err := store.Apply(ctx, userID, delta); err != nil {
			log.Warn("stats apply failed", "userID", userID, "err", err)
			return
		}
		log.Debug("stats updated",
			"userID", userID,
			"affDelta", delta.Affinity,
			"moodDelta", delta.Mood,
		)
	}()
}
