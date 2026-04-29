// Package store 封装本项目对 Redis 的所有访问。
//
// 设计约束：
//  1. 历史、长期记忆、stats 与主动消息各自通过窄 Repository / Store 访问 Redis；
//     不把业务读写散落到调用方。
//  2. 所有 Redis key 都通过各自包内函数生成，禁止在业务代码里手写 key，
//     以便未来统一迁移 / rename。
//  3. 本包只暴露 interface（见 history.go），底层结构体不外泄。
package store

import (
	"context"
	"fmt"

	"github.com/echo/llm-bot/internal/config"
	"github.com/redis/go-redis/v9"
)

// NewRedisClient 根据配置创建一个可用的 *redis.Client。
//
// 调用方在 main.go 中 defer Close()。本包不做连接池参数暴露，使用
// go-redis 默认值足以。
func NewRedisClient(ctx context.Context, cfg config.Redis) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("store: redis ping: %w", err)
	}
	return rdb, nil
}
