// Package store 的 group_buffer.go 声明"短期群聊上下文缓存"对外可见的契约：
// 实体 GroupBufferEntry 与仓库接口 GroupBufferRepo。
//
// 接口与底层实现刻意分文件：
//   - Agent 层只依赖接口，写入侧（onebot 适配器）也只依赖接口，方便单测用桩件替身；
//   - Redis 实现 + 构造器（NewGroupBufferRepo）由独立文件提供，避免一处改动牵动所有
//     消费者；
//   - 在 Append/Load 之外，不开放底层细节（key 形态、LTRIM/EXPIRE 时机等）。
package store

import (
	"context"
	"time"
)

// GroupBufferEntry 是短期群聊上下文缓存中的一条记录。
//
// 与 history.historyEntry 不同：history 走的是"主链 user/assistant 双向对话"
// 的 schema.Message 模型；group buffer 只记录"群里发生了什么"，是给主模型在
// 提示词里渲染"刚才群里在聊什么"用的，因此字段刻意精简到 (谁 / 说了什么 /
// 什么时候)，不带 Role。
type GroupBufferEntry struct {
	// UserID 发言者的稳定标识。
	UserID string
	// UserName 发言者的展示名（昵称/群名片）。可能为空，调用方按"UserID 兜底"使用。
	UserName string
	// Content 发言文本（原始内容，未做时间前缀化）。
	Content string
	// Time 发言时间。零值代表上游写入时未提供时间——下游渲染需要兼容这种情况。
	Time time.Time
}

// GroupBufferRepo 抽象短期群聊上下文缓存的读写。
//
// 写入由 Bot 层在 follow-up gate 决定"不进 Graph"的群聊普通消息分支调用
// （见 internal/app/bot.cacheGroupBackground）；读取由 Agent 层 loadContext
// 节点调用，渲染成系统提示词中的"群聊背景"块。规模管控（最多保留多少条 /
// TTL）由实现侧负责（LTRIM + EXPIRE），消费者不需要关心。
type GroupBufferRepo interface {
	// Append 追加一条群聊发言到指定会话的缓存。
	// 调用方传入的 sessionID 已带 group_ 前缀（与 history 同源）。
	Append(ctx context.Context, sessionID, userID, userName, content string) error

	// Load 读取指定会话当前缓存的全部条目，时间从旧到新返回。
	// 若 session 不存在或缓存为空，返回空切片（非错误）。
	Load(ctx context.Context, sessionID string) ([]GroupBufferEntry, error)
}
