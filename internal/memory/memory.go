// Package memory 管理机器人对单个用户的长期事实记忆。
//
// 与 history 的短期逐条记录不同，memory 只保存一段压缩后的事实摘要：
//   - 维度是 "平台 + 用户"，同一个人在私聊和群聊里共享同一份记忆；
//   - Redis key 永不过期，避免用户隔很久回来时彻底失去身份锚；
//   - 内容始终限制在 maxChars 以内，防止长期累积把 system prompt 撑爆。
//
// memory 是装饰性能力：读写或模型更新失败都只打日志，不阻断主对话。
package memory

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/llmjson"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

const (
	keyPrefix       = "bot_memory_"
	dispatchTimeout = 15 * time.Second
)

// Store 封装长期记忆的 Redis 读写。
type Store struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewStore 构造长期记忆存储。
func NewStore(rdb *redis.Client, log *slog.Logger) *Store {
	return &Store{rdb: rdb, log: cmp.Or(log, slog.Default())}
}

// Load 读取指定用户的长期记忆。不存在或读取失败时返回空字符串。
func (s *Store) Load(ctx context.Context, platform, userID string, maxChars int) string {
	if s == nil || s.rdb == nil || userID == "" {
		return ""
	}
	text, err := s.rdb.Get(ctx, keyFor(platform, userID)).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			s.log.Warn("memory load failed",
				"platform", platform, "userID", userID, "err", err)
		}
		return ""
	}
	return limitChars(strings.TrimSpace(text), maxChars)
}

// Save 覆盖写入指定用户的长期记忆。空内容会删除旧记忆。
func (s *Store) Save(ctx context.Context, platform, userID, text string, maxChars int) error {
	if s == nil || s.rdb == nil || userID == "" {
		return nil
	}
	key := keyFor(platform, userID)
	text = limitChars(strings.TrimSpace(text), maxChars)
	if text == "" {
		if err := s.rdb.Del(ctx, key).Err(); err != nil {
			return fmt.Errorf("memory: delete: %w", err)
		}
		return nil
	}
	if err := s.rdb.Set(ctx, key, text, 0).Err(); err != nil {
		return fmt.Errorf("memory: set: %w", err)
	}
	return nil
}

func keyFor(platform, userID string) string {
	return keyPrefix + cmp.Or(platform, "unknown") + "_" + userID
}

type updatePromptFile struct {
	System string `yaml:"system"`
}

// LoadUpdatePrompt 读取长期记忆更新模型的 system prompt。
func LoadUpdatePrompt(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("memory: read update prompt file %s: %w", path, err)
	}
	var pf updatePromptFile
	if err := yaml.Unmarshal(raw, &pf); err != nil {
		return "", fmt.Errorf("memory: parse update prompt yaml: %w", err)
	}
	system := strings.TrimSpace(pf.System)
	if system == "" {
		return "", fmt.Errorf("memory: update prompt system is required")
	}
	return system, nil
}

type updateResp struct {
	Memory *string `json:"memory"`
}

// Update 让小模型把本轮对话合并进当前长期记忆。
func Update(ctx context.Context, m model.BaseChatModel, systemPrompt, currentMemory, query, reply string, maxChars int) (string, error) {
	messages := []*schema.Message{
		schema.SystemMessage(systemPrompt),
		schema.UserMessage(buildUpdateInput(currentMemory, query, reply, maxChars)),
	}
	msg, err := m.Generate(ctx, messages)
	if err != nil {
		return "", fmt.Errorf("memory: update generate: %w", err)
	}

	return parseUpdateContent(msg.Content, maxChars)
}

func parseUpdateContent(content string, maxChars int) (string, error) {
	raw := llmjson.StripFence(strings.TrimSpace(content))
	var resp updateResp
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return "", fmt.Errorf("memory: parse update json %q: %w", raw, err)
	}
	if resp.Memory == nil {
		return "", fmt.Errorf("memory: parse update json %q: missing memory field", raw)
	}
	return limitChars(strings.TrimSpace(*resp.Memory), maxChars), nil
}

func buildUpdateInput(currentMemory, query, reply string, maxChars int) string {
	var b strings.Builder
	b.WriteString("最大记忆长度：")
	b.WriteString(fmt.Sprint(maxChars))
	b.WriteString(" 字符\n\n<current_memory>\n")
	if strings.TrimSpace(currentMemory) == "" {
		b.WriteString("（无）")
	} else {
		b.WriteString(currentMemory)
	}
	b.WriteString("\n</current_memory>\n<user>\n")
	b.WriteString(query)
	b.WriteString("\n</user>\n<bot>\n")
	b.WriteString(reply)
	b.WriteString("\n</bot>")
	return b.String()
}

// Dispatch 异步执行长期记忆更新，不阻塞主回复链路。
func Dispatch(store *Store, m model.BaseChatModel, updatePrompt string, log *slog.Logger, platform, userID, currentMemory, query, reply string, maxChars int) {
	log = cmp.Or(log, slog.Default())
	if store == nil || m == nil || strings.TrimSpace(updatePrompt) == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), dispatchTimeout)
		defer cancel()

		latestMemory := store.Load(ctx, platform, userID, maxChars)
		if latestMemory == "" {
			latestMemory = currentMemory
		}

		next, err := Update(ctx, m, updatePrompt, latestMemory, query, reply, maxChars)
		if err != nil {
			log.Warn("memory update failed",
				"platform", platform, "userID", userID, "err", err)
			return
		}
		if strings.TrimSpace(next) == strings.TrimSpace(latestMemory) {
			return
		}
		if err := store.Save(ctx, platform, userID, next, maxChars); err != nil {
			log.Warn("memory save failed",
				"platform", platform, "userID", userID, "err", err)
		}
	}()
}

func limitChars(s string, maxChars int) string {
	if maxChars <= 0 {
		return strings.TrimSpace(s)
	}
	runes := []rune(strings.TrimSpace(s))
	if len(runes) <= maxChars {
		return string(runes)
	}
	return string(runes[:maxChars])
}
