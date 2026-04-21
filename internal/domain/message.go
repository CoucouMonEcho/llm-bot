// Package domain 定义整个项目通用、平台无关的核心数据结构。
//
// 这一层的设计目标：
//  1. 让 Adapter（任何 IM 平台）都能把原生协议转换成本层统一模型；
//  2. 让 Agent 层完全不关心消息来自 QQ、微信还是 Telegram；
//  3. 字段少而稳定——扩展用 RawEvent（原始事件）承载，而不是在本层不断加字段。
package domain

// Platform 标识消息来源平台。将来新增平台只需要增加常量。
type Platform string

const (
	// PlatformOneBot 代表 OneBot v11 协议（NapCatQQ / go-cqhttp 等）。
	PlatformOneBot Platform = "onebot"
)

// ConversationType 区分私聊与群聊。
// 会话历史的 Redis key 构造依赖此字段。
type ConversationType string

const (
	// ConversationPrivate 表示私聊，SessionID 形如 "private:<user_id>"。
	ConversationPrivate ConversationType = "private"
	// ConversationGroup 表示群聊，SessionID 形如 "group:<group_id>"。
	ConversationGroup ConversationType = "group"
)

// InboundMessage 是从 IM 平台接收到的一条已归一化的用户消息。
// Adapter 负责解码原生协议并填充本结构体，下游只读。
type InboundMessage struct {
	// Platform 消息来源平台。
	Platform Platform
	// ConvType 会话类型：私聊或群聊。
	ConvType ConversationType
	// SessionID 会话唯一标识，Redis 历史 key 的一部分。
	// 私聊：private:<user_id>；群聊：group:<group_id>（群内所有成员共享同一会话）。
	SessionID string
	// UserID 触发本次消息的用户，用于日志与可能的鉴权。
	UserID string
	// UserName 触发用户的昵称，供 prompt 模板可选使用。
	UserName string
	// Text 已去除 @、前缀等的纯净文本，也就是喂给 LLM 的那一段。
	Text string
	// RawEvent 保留原始事件，便于 Adapter 在 Send 时回查相关字段
	// （例如 OneBot 的 group_id / user_id 用于 send_msg）。
	RawEvent any
}

// OutboundMessage 是 Agent 产出的待发送回复，由 Adapter 翻译成原生协议。
type OutboundMessage struct {
	// Platform 必须与触发本次回复的 InboundMessage.Platform 一致。
	Platform Platform
	// ConvType 与 InboundMessage.ConvType 相同。
	ConvType ConversationType
	// SessionID 与 InboundMessage.SessionID 相同。Adapter 据此选择发送目标。
	SessionID string
	// Text 回复内容，纯文本。
	Text string
	// ReplyTo 可选：指向待回复的原始消息 id（OneBot 下映射为 message_id）。
	// Adapter 实现方若支持引用回复则使用该字段。
	ReplyTo string
}
