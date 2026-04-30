// Package onebot 的 server.go 负责 OneBot v11 反向 WebSocket 服务的生命周期。
//
// 运行时结构：
//
//	NapCat  ──WS握手──►  HTTP server (gorilla/websocket)
//	        ◄──JSON──►   session goroutine (每连接一个)
//	                       ├── readLoop:   反序列化事件 → 触发过滤 → 推 Receive channel
//	                       └── writeMu:    串行化 action 发送
//
// 设计考量：
//  1. 一个进程可以接多个 NapCat 实例（理论上），但同一时刻只缓存最近一个
//     活跃连接用于回发。Send 的目标选择依据是"最近一次收到消息的那条连接"。
//  2. 断线重连由 NapCat 自己负责，我们不做客户端侧的重连——反向 WS 下
//     服务端是被动方。
package onebot

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/domain"
	"github.com/gorilla/websocket"
)

// recvChanBuffer 是 Receive channel 的缓冲大小。
// 设置为 32 足以应对常见峰值；过大会掩盖下游处理慢的真实问题。
const recvChanBuffer = 32

// writeTimeout 是单次 WS 写操作的截止时间。
// OneBot action 通常很小，1 秒已非常宽松。
const writeTimeout = 3 * time.Second

// Adapter 是 onebot 包对外的唯一实现，满足 adapter.Adapter 接口。
type Adapter struct {
	cfg       config.Server
	trigger   config.Trigger
	blacklist config.Blacklist
	logger    *slog.Logger

	upgrader websocket.Upgrader

	// recv 投递解码后的消息给上层。Stop 时会被关闭。
	recv chan *domain.InboundMessage

	// activeConn 指向"最近一次收到消息的" NapCat 连接，用于 Send 回发。
	// 读写都要加 connMu。
	activeConn *websocket.Conn
	connMu     sync.Mutex

	// writeMu 保证同一条连接上的写操作是串行的（WS 协议要求同一帧不能并发写）。
	writeMu sync.Mutex

	// httpSrv 持有底层 http 服务器，Stop 时用于优雅关闭。
	httpSrv *http.Server

	// started / stopped 用于幂等控制。
	startOnce sync.Once
	stopOnce  sync.Once
}

// New 构造一个 OneBot Adapter。
//
// 参数：
//   - srvCfg：HTTP/WS 监听参数；
//   - tr：触发规则（会传给事件解码器做源头过滤）；
//   - blacklist：用户黑名单（会传给事件解码器做源头过滤）；
//   - logger：结构化日志器。
func New(srvCfg config.Server, tr config.Trigger, blacklist config.Blacklist, logger *slog.Logger) *Adapter {
	return &Adapter{
		cfg:       srvCfg,
		trigger:   tr,
		blacklist: blacklist,
		logger:    logger.With(slog.String("component", "adapter.onebot")),
		upgrader: websocket.Upgrader{
			// NapCat 是同机或内网部署，CORS 不构成安全问题。
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		recv: make(chan *domain.InboundMessage, recvChanBuffer),
	}
}

// Name 实现 adapter.Adapter。
func (a *Adapter) Name() string { return "onebot" }

// Start 启动 HTTP 服务并挂载 ws_path。非阻塞。
// 真正的长运行循环在 http.Serve 的 goroutine 中；本函数负责把 ListenAndServe
// 的错误泵到 logger。
//
// ctx 被取消时会触发优雅关闭（等价于调用 Stop）。
// 当前实现永远返回 nil——监听失败是后台 goroutine 内事件，通过 logger 暴露，
// 不走返回值。保留 error 返回签名是为了未来做"监听失败同步回传"预留。
func (a *Adapter) Start(ctx context.Context) error {
	a.startOnce.Do(func() {
		mux := http.NewServeMux()
		mux.HandleFunc(a.cfg.WSPath, a.handleWS)

		a.httpSrv = &http.Server{
			Addr:    a.cfg.Addr,
			Handler: mux,

			ReadHeaderTimeout: 5 * time.Second,
		}

		go func() {
			a.logger.Info("onebot ws server listening",
				slog.String("addr", a.cfg.Addr),
				slog.String("path", a.cfg.WSPath))
			if err := a.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				a.logger.Error("onebot ws server exited with error", slog.Any("err", err))
			}
		}()

		// ctx 结束后优雅关闭。
		go func() {
			<-ctx.Done()
			_ = a.Stop(context.Background())
		}()
	})
	return nil
}

// Stop 优雅关闭服务与所有连接，并关闭 recv channel。幂等。
func (a *Adapter) Stop(ctx context.Context) error {
	var stopErr error
	a.stopOnce.Do(func() {
		if a.httpSrv != nil {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			stopErr = a.httpSrv.Shutdown(shutdownCtx)
		}
		a.connMu.Lock()
		if a.activeConn != nil {
			_ = a.activeConn.Close()
			a.activeConn = nil
		}
		a.connMu.Unlock()
		close(a.recv)
		a.logger.Info("onebot ws server stopped")
	})
	return stopErr
}

// Receive 实现 adapter.Adapter。
func (a *Adapter) Receive() <-chan *domain.InboundMessage { return a.recv }

// Send 把 OutboundMessage 翻译成 OneBot action 并写到 active connection。
// 若当前没有活跃连接，返回错误——消息丢失但保留可观测。
func (a *Adapter) Send(_ context.Context, out *domain.OutboundMessage) error {
	payload, err := buildSendAction(out)
	if err != nil {
		return err
	}

	a.connMu.Lock()
	conn := a.activeConn
	a.connMu.Unlock()
	if conn == nil {
		return fmt.Errorf("onebot: no active ws connection, drop %s", out.SessionID)
	}

	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("onebot: write action: %w", err)
	}
	return nil
}

// handleWS 是 HTTP handler：完成 access_token 校验后升级到 WS 并进入 readLoop。
func (a *Adapter) handleWS(w http.ResponseWriter, r *http.Request) {
	// 步骤 1：校验 access_token（NapCat 会把 token 放在 Authorization 头里）。
	if a.cfg.AccessToken != "" {
		// 优先 Authorization Bearer，空时兜底到 X-Self-Token（部分客户端走这个头）。
		token := cmp.Or(extractBearer(r.Header.Get("Authorization")), r.Header.Get("X-Self-Token"))
		if token != a.cfg.AccessToken {
			a.logger.Warn("onebot ws auth failed", slog.String("remote", r.RemoteAddr))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// 步骤 2：升级到 WebSocket。
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		a.logger.Error("onebot ws upgrade failed", slog.Any("err", err))
		return
	}
	a.logger.Info("onebot ws connected", slog.String("remote", r.RemoteAddr))

	// 步骤 3：读取 X-Self-ID 头取得机器人自己的 QQ 号，用于 @ 识别。
	selfID, _ := strconv.ParseInt(r.Header.Get("X-Self-ID"), 10, 64)

	// 步骤 4：登记为 active connection；同一进程内只保留最近一条。
	a.connMu.Lock()
	if a.activeConn != nil {
		_ = a.activeConn.Close()
	}
	a.activeConn = conn
	a.connMu.Unlock()

	// 步骤 5：进入 readLoop，直到连接出错或对端关闭。
	a.readLoop(conn, selfID)
}

// readLoop 在单连接上循环读取消息：
//  1. 读一条 WS 帧；
//  2. 交给 decodeAndFilter 解码并做触发过滤；
//  3. 非 nil 结果投递进 recv channel。
//
// 任何读失败（含 EOF）都导致本循环退出并把该连接从 activeConn 解除登记。
func (a *Adapter) readLoop(conn *websocket.Conn, selfID int64) {
	defer func() {
		a.connMu.Lock()
		if a.activeConn == conn {
			a.activeConn = nil
		}
		a.connMu.Unlock()
		_ = conn.Close()
		a.logger.Info("onebot ws disconnected")
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				a.logger.Warn("onebot ws read error", slog.Any("err", err))
			}
			return
		}

		msg, decodeErr := decodeAndFilter(data, selfID, a.trigger, a.blacklist)
		if decodeErr != nil {
			a.logger.Warn("onebot decode error",
				slog.Any("err", decodeErr),
				slog.String("raw", truncate(string(data), 200)))
			continue
		}
		if msg == nil {
			// 非触发事件或心跳，静默丢弃。
			continue
		}

		// 投递到 channel。两段式写入的动机：
		//  1. 快路径（channel 未满）直接走 case 分支，零日志、零额外开销；
		//  2. 满时才进入 default 打一条 warn，然后再阻塞写入——这样"反压发生"
		//     可被 log 系统直接观测，而不是被静默吞掉。
		// 仅有一次 warn（而不是每帧打）——后续若持续拥塞会在下一次满 channel 时再报。
		select {
		case a.recv <- msg:
		default:
			a.logger.Warn("onebot recv channel full, blocking",
				slog.String("session", msg.SessionID))
			a.recv <- msg
		}
	}
}

// extractBearer 从 Authorization 头取出 token 值。
//
// 兼容两种形态：
//   - 标准形态 "Bearer xxx" —— 剥掉前缀后返回 "xxx"；
//   - 非标准形态（NapCat 某些配置直接把 token 塞到 Authorization）——原样返回。
//
// 安全说明：非标准形态意味着任何 Authorization 头值都会被当成候选 token 参与
// 等值比较。由于上层调用处使用 `token != a.cfg.AccessToken` 做校验且
// AccessToken 非空时才校验，原样返回不会削弱校验强度；攻击者仍需猜出完整
// 的 AccessToken 字符串才能通过。
func extractBearer(h string) string {
	const prefix = "Bearer "
	if len(h) > len(prefix) && h[:len(prefix)] == prefix {
		return h[len(prefix):]
	}
	return h
}

// truncate 对日志字符串做最大长度截断，避免日志被一条巨大消息刷爆。
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
