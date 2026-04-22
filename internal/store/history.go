// Package store 的 history.go 定义并实现对话历史 Repository。
//
// 数据模型：
//   - 每一条消息以 JSON 形式序列化后 LPUSH 到一个 Redis List；
//   - key 形如 "bot:hist:<platform>:<sessionID>"；
//   - 每次写入后用 LTRIM 0 max-1 保留最新的 max 条，避免 List 无限增长；
//   - 读取时用 LRANGE 0 n-1 后再按"时间从旧到新"反转，以便喂给 LLM。
//
// 设计思考：
//   - 同一条对话的 user 消息与 assistant 消息都会追加进来，以保持顺序；
//   - 攻击消息与降级回复不会调用 Append（这一点在 agent 层控制，store 层
//     不感知业务语义）。
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

// redisHistoryRepo 是 HistoryRepo 的 Redis 实现。
type redisHistoryRepo struct {
	cli *redis.Client
	// keyPrefix 是所有历史 key 的公共前缀，默认 "bot:hist:"。
	// 将来若多租户部署，可通过构造器参数化。
	keyPrefix string
}

// NewHistoryRepo 构造一个基于 Redis 的 HistoryRepo。
func NewHistoryRepo(cli *redis.Client) HistoryRepo {
	return &redisHistoryRepo{
		cli:       cli,
		keyPrefix: "bot:hist:",
	}
}

// historyEntry 是写入 Redis 的序列化结构。
// 除 schema.Message 外额外记录时间戳，便于排查。
type historyEntry struct {
	Role    string    `json:"role"`
	Content string    `json:"content"`
	Time    time.Time `json:"ts"`
}

// Append 实现 HistoryRepo。
//
// 执行步骤：
//  1. 序列化 msg 为 JSON；
//  2. LPUSH 到 session 对应的 List（新消息在 index 0）；
//  3. 若 maxLen>0，用 LTRIM 保留 [0, maxLen-1]。
//
// 这两条写命令未使用 MULTI 封装：单条消息写入 + 裁剪的并发安全性对于
// "对话历史"而言不是严苛需求——偶尔多出一条不会造成正确性问题。
// 若将来需要严格 atomic，改为 pipeline + TxPipeline 即可。
func (r *redisHistoryRepo) Append(ctx context.Context, sessionID string, msg *schema.Message, maxLen int) error {
	if msg == nil {
		return fmt.Errorf("store: append nil message")
	}
	entry := historyEntry{
		Role:    string(msg.Role),
		Content: msg.Content,
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
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store: append history: %w", err)
	}
	return nil
}

// Load 实现 HistoryRepo。
//
// 注意返回顺序：Redis 中 index 0 是最新消息（LPUSH 的特性），但 LLM 需要
// 的是"从旧到新"的历史序列，所以读取后要反转一次。
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
		msgs = append(msgs, &schema.Message{
			Role:    schema.RoleType(entry.Role),
			Content: entry.Content,
		})
	}
	return msgs, nil
}

// keyFor 构造某个 sessionID 对应的 Redis key。
func (r *redisHistoryRepo) keyFor(sessionID string) string {
	return r.keyPrefix + sessionID
}
