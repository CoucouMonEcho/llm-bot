// Package persona manages lightweight global topic anchors used by the chat
// persona to naturally continue recent casual threads.
package persona

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/llmjson"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

const (
	// TopicsKey stores global reusable casual topic anchors. ZSET score is the
	// Unix timestamp of the latest updater refresh.
	TopicsKey       = "bot_persona_casual_topics"
	dispatchTimeout = 10 * time.Second
)

// Store wraps Redis operations for persona topic anchors.
type Store struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewStore constructs a topic store.
func NewStore(rdb *redis.Client, log *slog.Logger) *Store {
	return &Store{rdb: rdb, log: cmp.Or(log, slog.Default())}
}

// Load returns the newest non-expired topic anchors. Expired members are
// removed before loading so stale anchors do not resurface later.
func (s *Store) Load(ctx context.Context, maxItems int, maxAge time.Duration) ([]string, error) {
	if s == nil || s.rdb == nil || maxItems <= 0 {
		return nil, nil
	}
	if err := s.removeExpired(ctx, time.Now(), maxAge); err != nil {
		return nil, err
	}
	items, err := s.rdb.ZRevRange(ctx, TopicsKey, 0, int64(maxItems-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("persona topics: load: %w", err)
	}
	return normalizeTopics(items, maxItems), nil
}

// Apply replaces the active topic set with updater results: expired members are
// pruned, returned topics are refreshed via ZADD, and active topics omitted by
// the updater are removed via ZREM.
func (s *Store) Apply(ctx context.Context, topics []string, maxItems int, maxAge time.Duration, now time.Time) error {
	if s == nil || s.rdb == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if err := s.removeExpired(ctx, now, maxAge); err != nil {
		return err
	}

	retained := normalizeTopics(topics, maxItems)
	retainedSet := make(map[string]struct{}, len(retained))
	for _, topic := range retained {
		retainedSet[topic] = struct{}{}
	}

	active, err := s.activeTopics(ctx, now, maxAge)
	if err != nil {
		return err
	}
	toRemove := make([]any, 0, len(active))
	for _, topic := range active {
		if _, ok := retainedSet[topic]; !ok {
			toRemove = append(toRemove, topic)
		}
	}
	if len(toRemove) > 0 {
		if err := s.rdb.ZRem(ctx, TopicsKey, toRemove...).Err(); err != nil {
			return fmt.Errorf("persona topics: remove stale: %w", err)
		}
	}

	if len(retained) == 0 {
		return nil
	}
	members := make([]redis.Z, 0, len(retained))
	score := float64(now.Unix())
	for _, topic := range retained {
		members = append(members, redis.Z{Score: score, Member: topic})
	}
	if err := s.rdb.ZAdd(ctx, TopicsKey, members...).Err(); err != nil {
		return fmt.Errorf("persona topics: refresh: %w", err)
	}
	return nil
}

func (s *Store) removeExpired(ctx context.Context, now time.Time, maxAge time.Duration) error {
	if maxAge <= 0 {
		return nil
	}
	cutoff := now.Add(-maxAge).Unix()
	if err := s.rdb.ZRemRangeByScore(ctx, TopicsKey, "-inf", fmt.Sprint(cutoff)).Err(); err != nil {
		return fmt.Errorf("persona topics: remove expired: %w", err)
	}
	return nil
}

func (s *Store) activeTopics(ctx context.Context, now time.Time, maxAge time.Duration) ([]string, error) {
	if maxAge <= 0 {
		items, err := s.rdb.ZRange(ctx, TopicsKey, 0, -1).Result()
		if err != nil {
			return nil, fmt.Errorf("persona topics: load active: %w", err)
		}
		return items, nil
	}
	minScore := fmt.Sprint(now.Add(-maxAge).Unix() + 1)
	items, err := s.rdb.ZRangeByScore(ctx, TopicsKey, &redis.ZRangeBy{
		Min: minScore,
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("persona topics: load active: %w", err)
	}
	return items, nil
}

type updatePromptFile struct {
	System string `yaml:"system"`
}

// LoadUpdatePrompt reads the persona topic updater system prompt.
func LoadUpdatePrompt(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("persona topics: read update prompt file %s: %w", path, err)
	}
	var pf updatePromptFile
	if err := yaml.Unmarshal(raw, &pf); err != nil {
		return "", fmt.Errorf("persona topics: parse update prompt yaml: %w", err)
	}
	system := strings.TrimSpace(pf.System)
	if system == "" {
		return "", fmt.Errorf("persona topics: update prompt system is required")
	}
	return system, nil
}

type updateResp struct {
	Topics *[]string `json:"topics"`
}

// Update asks a small model to keep the current reusable topic anchors.
func Update(ctx context.Context, m model.BaseChatModel, systemPrompt string, currentTopics []string, query, reply string, maxItems int) ([]string, error) {
	messages := []*schema.Message{
		schema.SystemMessage(systemPrompt),
		schema.UserMessage(buildUpdateInput(currentTopics, query, reply, maxItems)),
	}
	msg, err := m.Generate(ctx, messages)
	if err != nil {
		return nil, fmt.Errorf("persona topics: update generate: %w", err)
	}
	return ParseUpdateContent(msg.Content, maxItems)
}

// ParseUpdateContent parses strict updater JSON and normalizes topic members
// before they can be written to Redis.
func ParseUpdateContent(content string, maxItems int) ([]string, error) {
	raw := llmjson.StripFence(strings.TrimSpace(content))
	var resp updateResp
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return nil, fmt.Errorf("persona topics: parse update json %q: %w", raw, err)
	}
	if resp.Topics == nil {
		return nil, fmt.Errorf("persona topics: parse update json %q: missing topics field", raw)
	}
	return normalizeTopics(*resp.Topics, maxItems), nil
}

func buildUpdateInput(currentTopics []string, query, reply string, maxItems int) string {
	var b strings.Builder
	b.WriteString("最多保留话题数：")
	b.WriteString(fmt.Sprint(maxItems))
	b.WriteString("\n\n<current_topics>\n")
	if len(currentTopics) == 0 {
		b.WriteString("[]")
	} else {
		encoded, _ := json.Marshal(currentTopics)
		b.Write(encoded)
	}
	b.WriteString("\n</current_topics>\n<user>\n")
	b.WriteString(query)
	b.WriteString("\n</user>\n<bot>\n")
	b.WriteString(reply)
	b.WriteString("\n</bot>")
	return b.String()
}

// Dispatch asynchronously refreshes persona topic anchors after a normal reply.
func Dispatch(store *Store, m model.BaseChatModel, updatePrompt string, log *slog.Logger, query, reply string, maxItems int, maxAge time.Duration) {
	log = cmp.Or(log, slog.Default())
	if store == nil || m == nil || strings.TrimSpace(updatePrompt) == "" || maxItems <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()

		current, err := store.Load(ctx, maxItems, maxAge)
		if err != nil {
			log.Warn("persona topics load failed", "err", err)
			return
		}

		next, err := Update(ctx, m, updatePrompt, current, query, reply, maxItems)
		if err != nil {
			log.Warn("persona topics update failed", "err", err)
			return
		}
		if err := store.Apply(ctx, next, maxItems, maxAge, time.Now()); err != nil {
			log.Warn("persona topics apply failed", "err", err)
		}
	}()
}

func normalizeTopics(topics []string, maxItems int) []string {
	if maxItems <= 0 {
		return nil
	}
	out := make([]string, 0, min(len(topics), maxItems))
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		topic = normalizeTopic(topic)
		if topic == "" {
			continue
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		out = append(out, topic)
		if len(out) >= maxItems {
			break
		}
	}
	return out
}

func normalizeTopic(topic string) string {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return ""
	}
	for _, adverb := range []string{"刚刚", "刚才", "今天", "上把", "昨晚"} {
		topic = strings.ReplaceAll(topic, adverb, "")
	}
	topic = strings.Join(strings.Fields(topic), " ")
	topic = strings.TrimFunc(topic, func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune("，。,.、；;：:！!？?~～-—_（）()[]【】\"'“”‘’", r)
	})
	return strings.TrimSpace(topic)
}
