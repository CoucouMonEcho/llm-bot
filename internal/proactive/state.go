// Package proactive 维护主动消息的状态读写、开场白生成与调度循环。
//
// 本文件集中定义 Redis key 与状态读写。设计上只保留两个 Redis 入口：
//   - `bot_proactive_enabled`：运行期开关（人工写入），只读不写；
//   - `bot_proactive_group_last_inbound`：HASH，field=`<sessionID>`，
//     value=Unix 秒。同时承担两份语义：HKEYS 即"已知群集合"、HVALS 即
//     "群里上一次真实活跃的时间"。
//
// 这一份 HASH 既是写入面（ActivityRecorder 收到群消息时刷新、Scheduler
// 发送成功后回写防自激发），也是读取面（Scheduler 选最久未活跃的群）。
// 不再维护"用户最近活跃 / 用户可触达会话 / 群白名单 / 日限额 / 会话冷却"
// 等多套索引——业务上只需要"群冷却 1h"，多余的状态徒增运维成本和一致性风险。
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

	// keyGroupLastInbound 同时承担"已知群集合"与"该群上次真实活跃时间"两份语义。
	// field 用 sessionID（已带 `group_` 前缀），value 是 Unix 秒。
	// 一次 HGETALL 即可拿到全部决策依据，避免再开第二份索引去维护一致性。
	keyGroupLastInbound = "bot_proactive_group_last_inbound"
)

// State 封装主动消息用到的 Redis key 与命令形态。
//
// 这里不做内存缓存，Redis 是唯一真相源；多进程部署时只要外层保证调度器单
// 实例，记录器可以在多个 bot 实例上安全并发写入（HSET 同 field 仅存最新值）。
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

// RecordGroupInbound 记录某个群最近一次真实活跃的时间。
//
// 调用点有两个：
//  1. ActivityRecorder 收到任意群入站消息；
//  2. Scheduler 主动发送成功后——避免刚主动开口后立刻再次命中冷却。
//
// 写入失败由调用方自行降级（旁路写入打 warn 即可），不阻断主对话链路。
func (s *State) RecordGroupInbound(ctx context.Context, sessionID string, at time.Time) error {
	if sessionID == "" {
		return fmt.Errorf("proactive: empty session id")
	}
	if err := s.rdb.HSet(ctx, keyGroupLastInbound, sessionID, at.Unix()).Err(); err != nil {
		return fmt.Errorf("proactive: record group inbound: %w", err)
	}
	return nil
}

// GroupsLastInbound 一次拿到所有已知群与各自上次活跃时间。
//
// HGETALL 把"已知群集合"和"每群最后活跃时间"合并成一次 RTT；调度器据此
// 直接挑出最久未活跃的群。解析失败的 field 会被跳过并 warn——单条坏数据
// 不应该让整轮调度退化为不发送。冷启动时 HASH 不存在则返回空 map（非错误）。
func (s *State) GroupsLastInbound(ctx context.Context) (map[string]time.Time, error) {
	vals, err := s.rdb.HGetAll(ctx, keyGroupLastInbound).Result()
	if err != nil {
		return nil, fmt.Errorf("proactive: read group last inbound: %w", err)
	}
	out := make(map[string]time.Time, len(vals))
	for sessionID, raw := range vals {
		unix, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			s.log.Warn("proactive group last inbound parse failed",
				"session", sessionID, "value", raw, "err", err)
			continue
		}
		out[sessionID] = time.Unix(unix, 0)
	}
	return out, nil
}
