// Package proactive 维护主动消息的状态读写、开场白生成与调度循环。
//
// 本文件集中定义 Redis key 与状态读写。设计上只保留两个 Redis 入口：
//   - `bot_proactive_enabled`：运行期开关（人工写入），只读不写；
//   - `bot_proactive_group_last_spoke`：HASH，field=`<sessionID>`，
//     value=Unix 秒，表示 bot 上次在该群成功发言的时间。
//
// 这一份 HASH 由 bot 群消息发送成功后写入，也是 Scheduler 选最久未开口群
// 的读取面。
// 不再维护"用户最近活跃 / 用户可触达会话 / 群白名单 / 日限额 / 会话冷却"
// 等多套索引——业务上只需要"bot 群内沉默 1h"，多余的状态徒增运维成本和一致性风险。
//
// 本包不感知 YAML 启停：`Enabled` 只读 Redis 开关；运维通过 `redis-cli SET
// bot_proactive_enabled true` 临时启停，无需重启进程。
package proactive

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// keyEnabled 是运行期开关，由运维显式 SET；缺失或非法值都按"关闭"处理。
	keyEnabled = "bot_proactive_enabled"

	// keyGroupLastSpoke 记录 bot 在每个群里上次成功发言的时间。
	// field 用 sessionID（已带 `group_` 前缀），value 是 Unix 秒。
	// 一次 HGETALL 即可拿到全部决策依据，避免再开第二份索引去维护一致性。
	keyGroupLastSpoke = "bot_proactive_group_last_spoke"
)

// State 封装主动消息用到的 Redis key 与命令形态。
//
// 这里不做内存缓存，Redis 是唯一真相源；多进程部署时只要外层保证调度器单
// 实例，多个 bot 实例也可以安全并发写入（HSET 同 field 仅存最新值）。
type State struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewState 构造主动消息状态层。
func NewState(rdb *redis.Client, log *slog.Logger) *State {
	return &State{rdb: rdb, log: cmp.Or(log, slog.Default())}
}

// Enabled 读取运行期开关；缺失或格式异常都按关闭处理。
//
// 运行期开关故意和 YAML 配置开关分离：配置决定功能是否接线，Redis 开关让
// 运维可以在不重启进程的情况下临时启停主动消息。
func (s *State) Enabled(ctx context.Context) (bool, error) {
	raw, err := s.rdb.Get(ctx, keyEnabled).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("proactive: read enabled: %w", err)
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		s.log.Warn("proactive enabled switch parse failed", "value", raw, "err", err)
		return false, nil
	}
	return enabled, nil
}

// RecordGroupBotSpoke 记录 bot 在某个群里成功发言的时间。
//
// 写入失败由调用方自行降级（旁路写入打 warn 即可），不阻断主对话链路。
func (s *State) RecordGroupBotSpoke(ctx context.Context, sessionID string, at time.Time) error {
	if sessionID == "" {
		return fmt.Errorf("proactive: empty session id")
	}
	if err := s.rdb.HSet(ctx, keyGroupLastSpoke, sessionID, at.Unix()).Err(); err != nil {
		return fmt.Errorf("proactive: record group bot spoke: %w", err)
	}
	return nil
}

// GroupsLastBotSpoke 一次拿到所有候选群与 bot 各自上次发言时间。
//
// HGETALL 把"候选群集合"和"每群最后发言时间"合并成一次 RTT；调度器据此
// 直接挑出 bot 最久未开口的群。解析失败的 field 会被跳过并 warn——单条坏数据
// 不应该让整轮调度退化为不发送。冷启动时 HASH 不存在则返回空 map（非错误）。
func (s *State) GroupsLastBotSpoke(ctx context.Context) (map[string]time.Time, error) {
	vals, err := s.rdb.HGetAll(ctx, keyGroupLastSpoke).Result()
	if err != nil {
		return nil, fmt.Errorf("proactive: read group last bot spoke: %w", err)
	}
	out := make(map[string]time.Time, len(vals))
	for sessionID, raw := range vals {
		unix, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			s.log.Warn("proactive group last bot spoke parse failed",
				"session", sessionID, "value", raw, "err", err)
			continue
		}
		out[sessionID] = time.Unix(unix, 0)
	}
	return out, nil
}
