// Package onebot 实现 OneBot v11 协议的反向 WebSocket 接入（NapCatQQ、
// go-cqhttp 等都遵循这套协议）。
//
// 本文件只负责"数据结构 + 解码 + 触发过滤"，与网络 I/O 完全解耦，
// 以便单元测试可以直接喂 JSON 验证解码正确性。
package onebot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/domain"
)

// postTypeMessage 是 OneBot 普通聊天消息事件的 post_type 值。
const postTypeMessage = "message"

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
// 本函数在 Adapter 中作为"源头过滤器"使用：非触发事件会返回 (nil, nil)
// 而不是错误；调用方据此区分"应当忽略"和"应当打日志的解码错误"。
//
// 处理步骤：
//  1. JSON 解码到 rawEvent；
//  2. 只接受 post_type == "message" 的事件，其他（心跳、元事件）全部忽略；
//  3. 根据配置的 trigger 规则判断"该不该理"——
//     3.1 私聊：按 trigger.Private 决定；
//     3.2 群聊：先剥离 @bot 段，随后按 trigger.GroupAtOnly 与 prefix 决定；
//  4. 构建 InboundMessage，其中 Text 字段是**已经剥离触发标记后**的纯净文本。
func decodeAndFilter(raw []byte, selfID int64, tr config.Trigger) (*domain.InboundMessage, error) {
	var ev rawEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("onebot: unmarshal event: %w", err)
	}

	// Step 1: 过滤非消息事件（心跳 / meta_event / notice / request 等）。
	if ev.PostType != postTypeMessage {
		return nil, nil
	}

	// Step 2: 把 message 字段统一规约为 plainText + 是否 @ 了自己。
	plainText, atSelf, err := extractText(ev.Message, ev.RawMessage, selfID)
	if err != nil {
		return nil, fmt.Errorf("onebot: extract text: %w", err)
	}

	// Step 3: 按会话类型与配置做触发过滤。
	switch ev.MessageType {
	case "private":
		if !tr.Private {
			return nil, nil
		}
	case "group":
		// 群聊触发条件：@bot 或 以任一前缀开头（二者任一满足）。
		matched := false
		if atSelf {
			matched = true
		}
		plainText, matched = matchPrefix(plainText, tr.Prefix, matched)
		if tr.GroupAtOnly && !matched {
			return nil, nil
		}
		if !tr.GroupAtOnly && !matched {
			// 未配置 GroupAtOnly 也没命中前缀/@ 时，默认仍然不触发——
			// 群内闲聊若全部触发会被洗版。
			return nil, nil
		}
	default:
		// 其他消息类型（比如 discuss 已弃用）一律忽略。
		return nil, nil
	}

	plainText = strings.TrimSpace(plainText)
	if plainText == "" {
		return nil, nil
	}

	// Step 4: 组装 InboundMessage。
	convType := domain.ConversationPrivate
	sessionID := "private:" + strconv.FormatInt(ev.UserID, 10)
	if ev.MessageType == "group" {
		convType = domain.ConversationGroup
		sessionID = "group:" + strconv.FormatInt(ev.GroupID, 10)
	}

	userName := ev.Sender.Card
	if userName == "" {
		userName = ev.Sender.Nickname
	}

	return &domain.InboundMessage{
		Platform:  domain.PlatformOneBot,
		ConvType:  convType,
		SessionID: sessionID,
		UserID:    strconv.FormatInt(ev.UserID, 10),
		UserName:  userName,
		Text:      plainText,
		RawEvent:  &ev,
	}, nil
}

// extractText 从 OneBot 消息字段中提取纯文本并识别 @ 段。
//
// OneBot 有两种 message 编码形式：
//  1. 字符串形式（老协议 / CQ 码）："[CQ:at,qq=123] 你好"
//  2. 数组形式（新协议 / 默认）：[{"type":"at","data":{"qq":"123"}},{"type":"text","data":{"text":" 你好"}}]
//
// NapCat 默认使用数组形式，但容错保留字符串解析路径。
//
// 返回 plainText 与 atSelf（该消息是否 @ 了机器人自己）。
func extractText(msg json.RawMessage, fallback string, selfID int64) (string, bool, error) {
	if len(msg) > 0 {
		trim := strings.TrimSpace(string(msg))
		// 尝试数组形式
		if strings.HasPrefix(trim, "[") {
			var segs []messageSegment
			if err := json.Unmarshal(msg, &segs); err == nil {
				return extractFromSegments(segs, selfID)
			}
			// 数组形式解析失败则降级到字符串形式
		}
		// 尝试字符串形式
		var s string
		if err := json.Unmarshal(msg, &s); err == nil {
			return extractFromCQString(s, selfID)
		}
	}
	// fallback 到 raw_message（字符串，带 CQ 码）
	return extractFromCQString(fallback, selfID)
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

// extractFromCQString 处理字符串形式的消息（CQ 码）。
//
// 仅识别 [CQ:at,qq=xxx]。其余 CQ 码一律剥除而不报错。
func extractFromCQString(s string, selfID int64) (string, bool, error) {
	atSelf := false
	var sb strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '[' {
			end := strings.IndexByte(s[i:], ']')
			if end < 0 {
				sb.WriteByte(s[i])
				i++
				continue
			}
			token := s[i : i+end+1]
			if strings.HasPrefix(token, "[CQ:at,") {
				// 解析 qq 参数
				body := strings.TrimPrefix(token, "[CQ:at,")
				body = strings.TrimSuffix(body, "]")
				for _, kv := range strings.Split(body, ",") {
					if strings.HasPrefix(kv, "qq=") {
						qq, err := strconv.ParseInt(strings.TrimPrefix(kv, "qq="), 10, 64)
						if err == nil && qq == selfID {
							atSelf = true
						}
					}
				}
			}
			// 其他 CQ 码一律丢弃
			i += end + 1
			continue
		}
		sb.WriteByte(s[i])
		i++
	}
	return sb.String(), atSelf, nil
}

// matchPrefix 检查 text 是否以 prefixes 中任意一个开头；
// 若命中，剥掉前缀并把 matched 置为 true。
//
// alreadyMatched 传入的是"之前是否已经因 @ 命中"，用以短路继续检查前缀。
// 无论是否已经 matched 我们都会尝试剥一次前缀——让 "@bot /help" 这类场景也能
// 得到剥离后的干净文本。
func matchPrefix(text string, prefixes []string, alreadyMatched bool) (string, bool) {
	matched := alreadyMatched
	trimmed := strings.TrimSpace(text)
	for _, p := range prefixes {
		if strings.HasPrefix(trimmed, p) {
			trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, p))
			return trimmed, true
		}
	}
	return text, matched
}

// toInt64 把 any（可能是 json.Number / float64 / string）安全转为 int64。
func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case json.Number:
		return t.Int64()
	case float64:
		return int64(t), nil
	case int64:
		return t, nil
	case int:
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
// 我们不处理 echo 字段，也不关心 response：机器人发出去就完事，
// NapCat 的回执不参与业务流程。
func buildSendAction(out *domain.OutboundMessage) ([]byte, error) {
	switch out.ConvType {
	case domain.ConversationPrivate:
		userID, err := parseSessionIDSuffix(out.SessionID, "private:")
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
		groupID, err := parseSessionIDSuffix(out.SessionID, "group:")
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{
			"action": "send_group_msg",
			"params": map[string]any{
				"group_id": groupID,
				"message":  out.Text,
			},
		})
	default:
		return nil, fmt.Errorf("onebot: unsupported conv type %q", out.ConvType)
	}
}

// parseSessionIDSuffix 从 "private:123" 形式的 SessionID 中提取数字部分。
func parseSessionIDSuffix(sessionID, prefix string) (int64, error) {
	s := strings.TrimPrefix(sessionID, prefix)
	if s == sessionID {
		return 0, fmt.Errorf("onebot: session id %q has no %q prefix", sessionID, prefix)
	}
	return strconv.ParseInt(s, 10, 64)
}
