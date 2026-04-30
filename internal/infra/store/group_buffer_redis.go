// Package store 的 group_buffer_redis.go 提供 GroupBufferRepo 的 Redis 实现。
//
// 与 history.go 的 redisHistoryRepo 风格保持一致：
//   - 单 pipeline 完成 LPUSH + LTRIM + EXPIRE，写入即裁剪即续期；
//   - 不用 MULTI——多写一条 / TTL 早晚几毫秒对"刚才群里在聊什么"这类弱
//     一致需求毫无影响，TxPipeline 的额外开销不值得；
//   - key 命名沿用 '_' 分隔的项目惯例（"bot_groupbuf_<sessionID>"），跟
//     "bot_hist_" / "bot_proactive_" 一脉相承，扫 key 时不会记混。
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// 默认值仅用于"调用方传 <=0"的兜底，不直接暴露给配置层——配置层有自己的
// 默认机制（见 config.defaultGroupBuffer）。这里再兜一次是防御性代码：
// 即便外部直接 NewGroupBufferRepo(cli, 0, 0) 也不会写出空 List 或永不过期的 key。
const (
	defaultGroupBufferMax = 20
	defaultGroupBufferTTL = 10 * time.Minute
	groupBufferKeyPrefix  = "bot_groupbuf_"
)

// NewGroupBufferRepo 构造一个基于 Redis 的 GroupBufferRepo。
//
//   - maxMessages：每个 session 在 Redis List 里保留的最大条数；<=0 时按 20 兜底。
//     这是窗口大小，不是"最多写多少"——超出会在写入同一 pipeline 里被
//     LTRIM 吃掉最老的几条。
//   - ttl：每次 Append 后刷新的 EXPIRE；<=0 时按 10 分钟兜底。
//
// keyPrefix 内部固定 "bot_groupbuf_"，与 history / stats 一样使用 '_' 作分隔。
func NewGroupBufferRepo(cli *redis.Client, maxMessages int, ttl time.Duration) GroupBufferRepo {
	if maxMessages <= 0 {
		maxMessages = defaultGroupBufferMax
	}
	if ttl <= 0 {
		ttl = defaultGroupBufferTTL
	}
	return &redisGroupBufferRepo{
		cli:         cli,
		keyPrefix:   groupBufferKeyPrefix,
		maxMessages: maxMessages,
		ttl:         ttl,
	}
}

// redisGroupBufferRepo 是 GroupBufferRepo 的 Redis 实现。
type redisGroupBufferRepo struct {
	cli         *redis.Client
	keyPrefix   string
	maxMessages int
	ttl         time.Duration
}

// groupBufferEntry 是写入 Redis 的内部序列化结构，与对外的 GroupBufferEntry
// 隔离：将来若要扩 schema（例如加 message_id），导出类型可以稳，不会强制
// 调用方一起改。字段名和 history.historyEntry 都用短 tag，省点 List 体积。
type groupBufferEntry struct {
	UserID   string    `json:"uid"`
	UserName string    `json:"un,omitempty"`
	Content  string    `json:"c"`
	Time     time.Time `json:"ts"`
}

// Append 实现 GroupBufferRepo。
//
// 噪音过滤：sessionID/userID 任一为空、或 content TrimSpace 后仍为空时直接返回 nil，
// 不写 Redis——这些是上游事件解析失败 / 纯空白消息的常见情况，写进去也只是
// 给后续渲染添堵，不视作错误。
//
// 时间复杂度：单次 Append 是 O(1)（LTRIM 截尾常数项随 maxMessages 线性，
// 但 maxMessages 是配置上限通常 ≤ 几十条）。
func (r *redisGroupBufferRepo) Append(ctx context.Context, sessionID, userID, userName, content string) error {
	content = strings.TrimSpace(content)
	if sessionID == "" || userID == "" || content == "" {
		return nil
	}
	entry := groupBufferEntry{
		UserID:   userID,
		UserName: userName,
		Content:  content,
		Time:     time.Now().UTC(),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("store: marshal group buffer: %w", err)
	}
	key := r.keyFor(sessionID)

	pipe := r.cli.Pipeline()
	pipe.LPush(ctx, key, data)
	pipe.LTrim(ctx, key, 0, int64(r.maxMessages-1))
	// LPUSH 之后再 EXPIRE，保证 key 一定存在；与 history.go 选择一致。
	pipe.Expire(ctx, key, r.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store: append group buffer: %w", err)
	}
	return nil
}

// Load 实现 GroupBufferRepo。
//
// 用 LRANGE 0 -1 取整段 List（写入侧已经 LTRIM 到 maxMessages，体量可控），
// 然后倒序遍历得到"时间从旧到新"的切片：Redis 中 index 0 是最新消息，
// 而 LLM 期望按时间顺序看到对话。
//
// 单条解析失败仅跳过，不让一条脏数据把整个上下文废掉——历史损坏 /
// schema 升级期间的旧记录都靠这层兜底通过。
//
// 时间复杂度：O(n)，n ≤ maxMessages。
func (r *redisGroupBufferRepo) Load(ctx context.Context, sessionID string) ([]GroupBufferEntry, error) {
	if sessionID == "" {
		return nil, nil
	}
	key := r.keyFor(sessionID)
	raws, err := r.cli.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("store: lrange group buffer: %w", err)
	}
	if len(raws) == 0 {
		return nil, nil
	}
	out := make([]GroupBufferEntry, 0, len(raws))
	for i := len(raws) - 1; i >= 0; i-- {
		var entry groupBufferEntry
		if err := json.Unmarshal([]byte(raws[i]), &entry); err != nil {
			continue
		}
		out = append(out, GroupBufferEntry{
			UserID:   entry.UserID,
			UserName: entry.UserName,
			Content:  entry.Content,
			Time:     entry.Time,
		})
	}
	return out, nil
}

// keyFor 是包内辅助函数，保持小写不导出——key 命名是 store 内部的实现细节，
// 不能让外面拿着拼字符串去绕过 Repo 直读 Redis。
func (r *redisGroupBufferRepo) keyFor(sessionID string) string {
	return r.keyPrefix + sessionID
}
