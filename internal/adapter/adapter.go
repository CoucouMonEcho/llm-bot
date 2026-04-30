// Package adapter 定义 IM 平台接入的统一抽象。
//
// 每一个 IM 平台（OneBot/NapCatQQ、微信、Telegram...）都通过实现本包的
// Adapter 接口接入系统。上层（Bot 主循环、Agent）只认 Adapter，不关心协议。
//
// 关键设计：
//  1. Receive 返回只读 channel：adapter 按自己的节奏把解码后的
//     InboundMessage 投递进来，主循环用 range 消费；
//  2. Adapter 只做协议解码、黑名单过滤与触发元信息标记；是否进入 Agent
//     Graph 由 Bot 层结合会话窗口等上下文决定；
//  3. Send 是同步的：调用返回后消息已经（被业务视为）发送完成。
package adapter

import (
	"context"

	"github.com/echo/llm-bot/internal/domain"
)

// Adapter 是所有 IM 平台接入层的统一接口。
type Adapter interface {
	// Name 返回该 Adapter 的可读名称，用于日志和可观测。
	Name() string

	// Start 启动 Adapter 的接收循环；必须是非阻塞的（内部自起 goroutine），
	// 直到 ctx 被取消或发生不可恢复错误才返回 error。
	Start(ctx context.Context) error

	// Stop 优雅关闭 Adapter，关闭 Receive 返回的 channel，并释放底层资源。
	// 多次调用应当幂等。
	Stop(ctx context.Context) error

	// Receive 返回一个只读 channel，上游用 `for msg := range ad.Receive()` 消费。
	// adapter 内部的事件解码与黑名单过滤逻辑决定哪些消息会被投递。
	// 当 Stop 被调用后，channel 会被关闭，for-range 自然退出。
	Receive() <-chan *domain.InboundMessage

	// Send 把一条 OutboundMessage 发送到对应的 IM 平台。
	// 调用方应当使用带超时/取消的 ctx 以避免协议栈阻塞。
	Send(ctx context.Context, out *domain.OutboundMessage) error
}
