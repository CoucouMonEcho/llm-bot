// Package store 的 history.go 定义并实现对话历史 Repository。
//
// 数据模型：
//   - 每一条消息以 JSON 形式序列化后 LPUSH 到一个 Redis List；
//   - key 形如 "bot_hist_<sessionID>"（sessionID 自身已带 private_/group_ 前缀）；
//   - 每次写入后用 LTRIM 0 max-1 保留最新的 max 条，避免 List 无限增长；
//   - 每次写入后以滑动窗口 EXPIRE 30 天：活跃会话永不过期，长期沉默的会话
//     会自然回收，既省内存也让"隔半年回来"的上下文不会诡异地拼到当下对话中；
//   - 读取时用 LRANGE 0 n-1 后再按"时间从旧到新"反转，以便喂给 LLM。
//
// 设计思考：
//   - 同一条对话的 user 消息与 assistant 消息都会追加进来，以保持顺序；
//   - 攻击消息会在 agent 层静默中断，不会调用 Append；store 层不感知业务语义。
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cloudwego/eino/schema"
	"github.com/redis/go-redis/v9"
)

// HistoryRepo 抽象出历史存储操作，便于将来替换底层存储实现。
type HistoryRepo interface {
	// Append 将一条消息写入指定 session 的历史，并裁剪到 maxLen 条以内。
	// maxLen<=0 表示不裁剪（不推荐）。
	Append(ctx context.Context, sessionID string, msg *schema.Message, maxLen int) error

	// Load 读取最近 n 条历史，返回按时间从旧到新排序的切片。
	// 若 session 不存在则返回空切片（非错误）。
	Load(ctx context.Context, sessionID string, n int) ([]*schema.Message, error)
}

// historyTTL 是单条会话历史 key 的滑动窗口过期时间。
//
// 采用"每次 Append 都刷新 EXPIRE"的滑动窗口策略：
//   - 活跃用户的会话被每次对话持续续期，实际永不过期；
//   - 长期沉默（超过 30 天）的会话会被 Redis 自然清理，避免 key 无限堆积；
//   - 实现成本极低——只要在 LPUSH/LTRIM 同一 pipeline 里多加一条 EXPIRE 即可，
//     不需要先 EXISTS 探测或 EXPIRE NX。
const historyTTL = 30 * 24 * time.Hour

// redisHistoryRepo 是 HistoryRepo 的 Redis 实现。
type redisHistoryRepo struct {
	cli *redis.Client
	// keyPrefix 是所有历史 key 的公共前缀，默认 "bot_hist_"。
	// 全部用 '_' 作分隔而不是 ':'，是为了让项目里所有 Redis key 的层级分隔符
	// 统一（stats / history 都一样），扫 key 时不用记混。
	// 将来若多租户部署，可通过构造器参数化。
	keyPrefix string
}

// NewHistoryRepo 构造一个基于 Redis 的 HistoryRepo。
func NewHistoryRepo(cli *redis.Client) HistoryRepo {
	return &redisHistoryRepo{
		cli:       cli,
		keyPrefix: "bot_hist_",
	}
}

// historyEntry 是写入 Redis 的序列化结构。
// 除 schema.Message 外额外记录时间戳，便于排查。
type historyEntry struct {
	Role    string    `json:"role"`
	Content string    `json:"content"`
	Name    string    `json:"name,omitempty"`
	Time    time.Time `json:"ts"`
}

// Append 实现 HistoryRepo。
//
// 执行步骤（单次 pipeline 发送）：
//  1. 序列化 msg 为 JSON；
//  2. LPUSH 到 session 对应的 List（新消息在 index 0）；
//  3. 若 maxLen>0，用 LTRIM 保留 [0, maxLen-1]；
//  4. EXPIRE 刷新为 historyTTL——每次对话都续期的"滑动窗口"。
//
// 这组命令未使用 MULTI 封装：单条消息写入 + 裁剪 + 续期的并发安全性对于
// "对话历史"而言不是严苛需求——偶尔多出一条、TTL 早晚几毫秒都不构成正确性
// 问题。若将来需要严格 atomic，改为 TxPipeline 即可。
func (r *redisHistoryRepo) Append(ctx context.Context, sessionID string, msg *schema.Message, maxLen int) error {
	if msg == nil {
		return fmt.Errorf("store: append nil message")
	}
	entry := historyEntry{
		Role:    string(msg.Role),
		Content: msg.Content,
		Name:    msg.Name,
		Time:    time.Now(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("store: marshal history: %w", err)
	}
	key := r.keyFor(sessionID)

	pipe := r.cli.Pipeline()
	pipe.LPush(ctx, key, data)
	if maxLen > 0 {
		pipe.LTrim(ctx, key, 0, int64(maxLen-1))
	}
	// 滑动窗口续期：每次 Append 都刷新为 historyTTL，对长期不活跃的会话
	// 由 Redis 自然过期回收。LPUSH 之后执行，保证 key 一定存在，EXPIRE 生效。
	pipe.Expire(ctx, key, historyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store: append history: %w", err)
	}
	return nil
}

// Load 实现 HistoryRepo。
//
// 注意返回顺序：Redis 中 index 0 是最新消息（LPUSH 的特性），但 LLM 需要
// 的是"从旧到新"的历史序列，所以读取后要反转一次。
//
// 时间戳前缀化（"写读不对称"约束）：
//   - schema.Message 自身没有时间字段（eino@v0.8.11/schema/message.go 仅有
//     Role/Content/Name/MultiContent…），想让 LLM"看见"消息发生的时间，唯一
//     可行的注入点就是 Content。这里在 Load 返回时给每条消息的 Content 拼一个
//     `[YYYY-MM-DD HH:MM] ` 前缀（本地时区，分钟精度），proactive generator /
//     主链 buildMessages 等所有读历史的地方自动受益。
//   - 关键约束：写入路径完全不沾这件事——Append 收到的 *schema.Message.Content
//     仍是纯净文本，落到 Redis 里的 historyEntry.Content 也是纯净文本，时间戳
//     单独保存在 historyEntry.Time。前缀只在 Load 出口处即时拼接。
//   - 因此调用方拿到 Load 结果后，**不能再原样 Append 回去**，否则历史里会
//     出现 "[2026-04-30 14:30] [2026-04-29 09:12] 你好" 这种叠加前缀的污染。
//     当前代码不存在这种环路（saveHistory 写入的来源是 flow.Input.Query / 状态机
//     的纯净 Reply，永远不取自 Load）；将来若加 RAG/摘要把 Load 结果回灌写回，
//     必须在注入点先剥掉这层前缀。
//   - entry.Time.IsZero() 时跳过前缀，避免出现 `[0001-01-01 00:00]` 这种丑值——
//     兼容历史损坏 / 早期未带 ts 的旧记录。
func (r *redisHistoryRepo) Load(ctx context.Context, sessionID string, n int) ([]*schema.Message, error) {
	if n <= 0 {
		return nil, nil
	}
	key := r.keyFor(sessionID)
	// LRANGE key 0 n-1：取最新的 n 条（因为 LPUSH 把新消息放到 index 0）。
	raws, err := r.cli.LRange(ctx, key, 0, int64(n-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("store: lrange history: %w", err)
	}
	// 反转为时间从旧到新。
	msgs := make([]*schema.Message, 0, len(raws))
	for i := len(raws) - 1; i >= 0; i-- {
		var entry historyEntry
		if err := json.Unmarshal([]byte(raws[i]), &entry); err != nil {
			// 单条解析失败记录但不阻断；历史损坏不应让当前对话失败。
			continue
		}
		content := entry.Content
		if !entry.Time.IsZero() {
			content = "[" + entry.Time.Local().Format("2006-01-02 15:04") + "] " + content
		}
		msgs = append(msgs, &schema.Message{
			Role:    schema.RoleType(entry.Role),
			Content: content,
			Name:    entry.Name,
		})
	}
	return msgs, nil
}

// keyFor 构造某个 sessionID 对应的 Redis key。
func (r *redisHistoryRepo) keyFor(sessionID string) string {
	return r.keyPrefix + sessionID
}
