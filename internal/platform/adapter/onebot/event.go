// Package onebot 实现 OneBot v11 协议的反向 WebSocket 接入（NapCatQQ、
// go-cqhttp 等都遵循这套协议）。
//
// 本文件只负责"数据结构 + 解码 + 触发过滤"，与网络 I/O 完全解耦，
// 以便单元测试可以直接喂 JSON 验证解码正确性。
package onebot

import (
	"cmp"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/domain"
)

// postTypeMessage 是 OneBot 普通聊天消息事件的 post_type 值。
const postTypeMessage = "message"

// emptyTriggerPlaceholder 是"@bot 但正文为空"时塞给 Agent 的占位文本。
// 括号 + 第三人称叙述让 LLM 把它读成元事件而非字面用户输入；
// "但什么也没说"显式关掉 LLM 脑补具体内容的可能；
// 不复用"戳了戳"是为了给将来真正的 OneBot poke 事件留出命名空间。
const emptyTriggerPlaceholder = "（艾特了你 但什么也没说）"

// rawEvent 是 OneBot v11 消息事件的最小公共子集解码结构。
// 为了避免全字段建模，未用到的字段一律以 json.RawMessage 承载。
type rawEvent struct {
	PostType    string          `json:"post_type"`
	MessageType string          `json:"message_type"` // "private" | "group"
	SelfID      int64           `json:"self_id"`
	UserID      int64           `json:"user_id"`
	GroupID     int64           `json:"group_id,omitempty"`
	MessageID   int64           `json:"message_id"`
	RawMessage  string          `json:"raw_message"` // 文本形态（带 CQ 码）
	Message     json.RawMessage `json:"message"`     // 可能是 string 或 []segment
	Sender      sender          `json:"sender"`
}

// sender 是 OneBot 消息中的发送者信息，仅保留有用字段。
type sender struct {
	UserID   int64  `json:"user_id"`
	Nickname string `json:"nickname"`
	Card     string `json:"card"` // 群名片
}

// messageSegment 是 OneBot 数组形态的消息段。
//
// 文本段：{"type":"text","data":{"text":"..."}}
// @ 段：{"type":"at","data":{"qq":"123"}}
type messageSegment struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// decodeAndFilter 把一条原始 JSON 事件解码为 domain.InboundMessage。
//
// 本函数在 Adapter 中作为"源头过滤器"使用：应忽略事件会返回 (nil, nil)
// 而不是错误；调用方据此区分"应当忽略"和"应当打日志的解码错误"。
//
// 处理步骤：
//  1. JSON 解码到 rawEvent；
//  2. 只接受 post_type == "message" 的事件，其他（心跳、元事件）全部忽略；
//  3. 按 blacklist.user_ids 过滤用户；
//  4. 根据配置的 trigger 规则判断触发元信息——
//     4.1 私聊：按 trigger.Private 决定；
//     4.2 群聊：保留普通文本消息；若 @bot 或命中 prefix，则标记 ExplicitTrigger；
//     显式触发但正文为空时会用 emptyTriggerPlaceholder 代替，避免静默丢弃；
//  5. 构建 InboundMessage，其中 Text 字段是**已经剥离触发标记后**的纯净文本。
func decodeAndFilter(raw []byte, selfID int64, tr config.Trigger, blacklist config.Blacklist) (*domain.InboundMessage, error) {
	var ev rawEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("onebot: unmarshal event: %w", err)
	}

	// 步骤 1：过滤非消息事件（心跳 / meta_event / notice / request 等）。
	if ev.PostType != postTypeMessage {
		return nil, nil
	}

	if userBlacklisted(ev.UserID, blacklist) {
		return nil, nil
	}

	// 步骤 3：把 message 字段统一规约为 plainText + 是否 @ 了自己。
	plainText, atSelf, err := extractText(ev.Message, ev.RawMessage, selfID)
	if err != nil {
		return nil, fmt.Errorf("onebot: extract text: %w", err)
	}

	// 步骤 4：按会话类型与配置做触发过滤，并记录显式触发元信息。
	explicitTrigger := false
	switch ev.MessageType {
	case "private":
		if !tr.Private {
			return nil, nil
		}
	case "group":
		// 群聊普通文本也会进入下游；@bot 或前缀只作为显式触发元信息。
		var prefixMatched bool
		plainText, prefixMatched = matchPrefix(plainText, tr.Prefix)
		explicitTrigger = atSelf || prefixMatched
	default:
		// 其他消息类型（比如 discuss 已弃用）一律忽略。
		return nil, nil
	}

	plainText = strings.TrimSpace(plainText)
	if plainText == "" {
		if !explicitTrigger {
			// 私聊或普通群聊的纯空白消息：罕见，静默忽略。
			return nil, nil
		}
		// 群聊 @ / 前缀已命中但正文为空（典型"只点了 @ 没打字"），
		// 是用户的明确意图，不该静默丢弃。塞一个元事件占位符让 Agent
		// 按人设自然反问；保持 Adapter 的"业务无感"——不在这里写死回复文案。
		plainText = emptyTriggerPlaceholder
	}

	// 步骤 5：组装 InboundMessage。
	convType := domain.ConversationPrivate
	sessionID := "private_" + strconv.FormatInt(ev.UserID, 10)
	if ev.MessageType == "group" {
		convType = domain.ConversationGroup
		sessionID = "group_" + strconv.FormatInt(ev.GroupID, 10)
	}

	// 群名片（Card）优先于昵称（Nickname）：群聊里用户可能自定义了更亲切的群名片。
	userName := cmp.Or(ev.Sender.Card, ev.Sender.Nickname)

	return &domain.InboundMessage{
		Platform:        domain.PlatformOneBot,
		ConvType:        convType,
		SessionID:       sessionID,
		UserID:          strconv.FormatInt(ev.UserID, 10),
		UserName:        userName,
		MessageID:       strconv.FormatInt(ev.MessageID, 10),
		Text:            plainText,
		ExplicitTrigger: explicitTrigger,
	}, nil
}

func userBlacklisted(userID int64, blacklist config.Blacklist) bool {
	id := strconv.FormatInt(userID, 10)
	for _, blocked := range blacklist.UserIDs {
		if blocked == id {
			return true
		}
	}
	return false
}

// extractText 从 OneBot 消息字段中提取纯文本并识别 @ 段。
//
// 只解析数组形态的 message（NapCat / 现代 go-cqhttp 默认）；字符串形态
// 原样返回，不识别其中内嵌的 CQ 码 @ 段——避免再维护一套 CQ 码词法分析器。
// 代价是老协议客户端在群里 @ 机器人时 atSelf 永远为 false，只能靠前缀
// 触发群聊，相对可接受；老客户端请升级到数组形态。
//
// 返回 plainText 与 atSelf（该消息是否 @ 了机器人自己）。
func extractText(msg json.RawMessage, fallback string, selfID int64) (string, bool, error) {
	if len(msg) > 0 {
		trim := strings.TrimSpace(string(msg))
		if strings.HasPrefix(trim, "[") {
			var segs []messageSegment
			if err := json.Unmarshal(msg, &segs); err == nil {
				return extractFromSegments(segs, selfID)
			}
			// 数组形式解析失败则降级到字符串形态。
		}
		var s string
		if err := json.Unmarshal(msg, &s); err == nil {
			return s, false, nil
		}
	}
	// message 字段为空或无法解码时退到 raw_message 原文。
	return fallback, false, nil
}

// extractFromSegments 遍历数组形式的消息段。
func extractFromSegments(segs []messageSegment, selfID int64) (string, bool, error) {
	var sb strings.Builder
	atSelf := false
	for _, seg := range segs {
		switch seg.Type {
		case "text":
			var d struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(seg.Data, &d); err != nil {
				return "", false, err
			}
			sb.WriteString(d.Text)
		case "at":
			// @ 段格式：{"qq":"123"} 或 {"qq":123}；兼容 string/number。
			var d struct {
				QQ any `json:"qq"`
			}
			if err := json.Unmarshal(seg.Data, &d); err != nil {
				return "", false, err
			}
			qq, _ := toInt64(d.QQ)
			if qq == selfID {
				atSelf = true
			}
			// @ 段在纯文本里就省略，不污染 LLM 输入。
		default:
			// 图片、表情、文件等其他段类型忽略：当前项目只处理文本对话。
		}
	}
	return sb.String(), atSelf, nil
}

// matchPrefix 检查 text 是否以 prefixes 中任意一个开头；
// 若命中，剥掉前缀返回 (trimmed, true)；未命中则返回 (text, false)。
//
// 无论调用点之前是否已因 @ 命中都会尝试剥一次前缀——让 "@bot /help" 这类
// 场景也能得到剥离后的干净文本。
func matchPrefix(text string, prefixes []string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	for _, p := range prefixes {
		if strings.HasPrefix(trimmed, p) {
			trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, p))
			return trimmed, true
		}
	}
	return text, false
}

// toInt64 把 any（可能是 json.Number / float64 / string）安全转为 int64。
// 输入由 json.Unmarshal 填充的 any 字段产生：JSON number 一律是 float64，
// JSON string 是 string；json.Number 分支仅在未来切到 UseNumber 时才有意义，
// 现在保留它让那次切换不用再来动这里。
func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case json.Number:
		return t.Int64()
	case float64:
		return int64(t), nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("onebot: unsupported qq type %T", v)
	}
}

// buildSendAction 构造 send_private_msg / send_group_msg 的 action JSON。
//
// OneBot action 报文形如：
//
//	{"action":"send_group_msg","params":{"group_id":123,"message":"hi"}}
//
// 群聊场景下若 out.ReplyTo 非空，会把 message 升级为数组形态并在正文前
// 插入一个 at 段或 reply 段（由 ReplyMode 决定）；否则继续按字符串形态发送。
// 私聊忽略 ReplyTo——一对一天然无需指向。
//
// 我们不处理 echo 字段，也不关心 response：机器人发出去就完事，
// NapCat 的回执不参与业务流程。
func buildSendAction(out *domain.OutboundMessage) ([]byte, error) {
	switch out.ConvType {
	case domain.ConversationPrivate:
		userID, err := parseSessionIDSuffix(out.SessionID, "private_")
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{
			"action": "send_private_msg",
			"params": map[string]any{
				"user_id": userID,
				"message": out.Text,
			},
		})
	case domain.ConversationGroup:
		groupID, err := parseSessionIDSuffix(out.SessionID, "group_")
		if err != nil {
			return nil, err
		}
		message, err := buildGroupMessageField(out)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{
			"action": "send_group_msg",
			"params": map[string]any{
				"group_id": groupID,
				"message":  message,
			},
		})
	default:
		return nil, fmt.Errorf("onebot: unsupported conv type %q", out.ConvType)
	}
}

// buildGroupMessageField 根据 ReplyTo 组装群聊消息字段。
//
// 没有 ReplyTo 或 Mode 为 none 时保留原来的字符串形态，减少无谓的字段；
// 有 at / quote 时升级为 segment 数组：NapCat 对两种形态都兼容，
// 但只有数组形态能自然携带 at/reply 段。
func buildGroupMessageField(out *domain.OutboundMessage) (any, error) {
	if out.ReplyTo == nil || out.ReplyTo.Mode == "" || out.ReplyTo.Mode == domain.ReplyModeNone {
		return out.Text, nil
	}

	segs := make([]map[string]any, 0, 3)
	switch out.ReplyTo.Mode {
	case domain.ReplyModeAt:
		if out.ReplyTo.UserID == "" {
			return nil, fmt.Errorf("onebot: reply mode %q requires user id", out.ReplyTo.Mode)
		}
		segs = append(segs,
			map[string]any{
				"type": "at",
				"data": map[string]any{"qq": out.ReplyTo.UserID},
			},
			// 在 @ 段与正文之间补一个空格，避免与昵称紧贴。
			map[string]any{
				"type": "text",
				"data": map[string]any{"text": " "},
			},
		)
	case domain.ReplyModeQuote:
		if out.ReplyTo.MessageID == "" {
			return nil, fmt.Errorf("onebot: reply mode %q requires message id", out.ReplyTo.Mode)
		}
		segs = append(segs, map[string]any{
			"type": "reply",
			"data": map[string]any{"id": out.ReplyTo.MessageID},
		})
	default:
		return nil, fmt.Errorf("onebot: unsupported reply mode %q", out.ReplyTo.Mode)
	}

	segs = append(segs, map[string]any{
		"type": "text",
		"data": map[string]any{"text": out.Text},
	})
	return segs, nil
}

// parseSessionIDSuffix 从 "private_123" 形式的 SessionID 中提取数字部分。
func parseSessionIDSuffix(sessionID, prefix string) (int64, error) {
	s := strings.TrimPrefix(sessionID, prefix)
	if s == sessionID {
		return 0, fmt.Errorf("onebot: session id %q has no %q prefix", sessionID, prefix)
	}
	return strconv.ParseInt(s, 10, 64)
}
