// Package domain 定义整个项目通用、平台无关的核心数据结构。
//
// 这一层的设计目标：
//  1. 让 Adapter（任何 IM 平台）都能把原生协议转换成本层统一模型；
//  2. 让 Agent 层完全不关心消息来自 QQ、微信还是 Telegram；
//  3. 字段少而稳定——扩展用新增类型而非在本层不断加字段。
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
	// ConversationPrivate 表示私聊，SessionID 形如 "private_<user_id>"。
	ConversationPrivate ConversationType = "private"
	// ConversationGroup 表示群聊，SessionID 形如 "group_<group_id>"。
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
	// 私聊：private_<user_id>；群聊：group_<group_id>（群内所有成员共享同一会话）。
	SessionID string
	// UserID 触发本次消息的用户，用于日志与可能的鉴权。
	UserID string
	// UserName 触发用户的昵称，供 prompt 模板可选使用。
	UserName string
	// MessageID 平台原生的消息 ID，群聊"引用回复"场景下 Adapter 需要它。
	// 私聊场景一般可留空。
	MessageID string
	// Text 已去除 @、前缀等的纯净文本，也就是喂给 LLM 的那一段。
	Text string
	// ExplicitTrigger 表示平台层显式触发了 bot（例如 @bot 或命令前缀）。
	ExplicitTrigger bool
}

// ReplyMode 定义一条外发消息相对原消息的"指向"样式。
// 由 Bot 层逐条决定，Adapter 据此渲染协议段。
//
//   - ReplyModeAt    : 在正文前插入 @ 段（艾特发信人）；
//   - ReplyModeQuote : 在正文前插入 reply 段（引用原消息）；
//   - ReplyModeNone  : 纯文本，不加任何指向性修饰。
//     ReplyTarget.Mode == ReplyModeNone 与 OutboundMessage.ReplyTo == nil
//     在 Adapter 侧行为等价；后者更符合"这条就是无上下文主动消息"的语义。
type ReplyMode string

const (
	ReplyModeAt    ReplyMode = "at"
	ReplyModeQuote ReplyMode = "quote"
	ReplyModeNone  ReplyMode = "none"
)

// ReplyTarget 描述一条外发消息要"指向谁"的信息。
//
// OutboundMessage.ReplyTo 为 nil 时 Adapter 直接发送纯文本——
// 正是未来"主动发消息、不艾特任何人"的用法。
type ReplyTarget struct {
	// Mode 指向性样式；ReplyModeNone 时等价于不填 ReplyTo。
	Mode ReplyMode
	// UserID 原始发信人 ID，ReplyModeAt 必填。
	UserID string
	// MessageID 原始消息 ID，ReplyModeQuote 必填。
	MessageID string
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
	// ReplyTo 可选；仅群聊场景 Adapter 会据此生成 at / reply 段。
	// nil 表示发送纯文本（例如主动发消息）。
	ReplyTo *ReplyTarget
}
