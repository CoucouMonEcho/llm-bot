// Command bot 是 llm-bot 的进程入口。
//
// 职责（按执行顺序）：
//  1. 解析命令行 flag 找到 config.yaml；
//  2. 加载配置、构造 logger；
//  3. 连 Redis，构造 HistoryRepo；
//  4. 加载人设 YAML，构造 Persona；
//  5. 构造 stats 资源（若启用）：stats.Store 与打分 ChatModel；
//  6. 构造 Agent Runnable（这一步会构造主/裁判 ChatModel、编译 Graph）；
//  7. 构造 OneBot Adapter；
//  8. 把 Bot 主循环跑起来；
//  9. 监听 SIGINT/SIGTERM 做优雅关闭。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudwego/eino/components/model"
	"github.com/echo/llm-bot/internal/adapter/onebot"
	"github.com/echo/llm-bot/internal/agent"
	"github.com/echo/llm-bot/internal/bot"
	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/stats"
	"github.com/echo/llm-bot/internal/store"
)

// defaultShutdownTimeout 是优雅关闭阶段最多等待的时长。
// 超过这个时间仍有 in-flight 请求就放弃它们，保证进程能退出。
const defaultShutdownTimeout = 10 * time.Second

func main() {
	// Step 1: 解析 flag
	configPath := flag.String("config", "configs/config.yaml", "path to config yaml")
	flag.Parse()

	// Step 2: 加载配置 + logger
	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal("load config: %v", err)
	}
	logger := newLogger(cfg.Log.Level)
	logger.Info("config loaded",
		slog.String("path", *configPath),
		slog.String("model", cfg.LLM.Model),
		slog.String("judge_model", cfg.Judge.Model))

	// Step 3: 连 Redis
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisCli, err := store.NewRedisClient(ctx, cfg.Redis)
	if err != nil {
		fatal("connect redis: %v", err)
	}
	defer func() { _ = redisCli.Close() }()
	historyRepo := store.NewHistoryRepo(redisCli)

	// Step 4: 加载人设
	persona, err := agent.LoadPersona(cfg.Agent.PromptFile)
	if err != nil {
		fatal("load persona: %v", err)
	}

	// Step 5: 构造 stats 资源（若启用）
	// 产出两件东西:
	//   - statsStore：传给 agent.Build，用于 buildMessages 节点读参数快照；
	//   - scoreModel：传给 bot.New，用于 bot 层异步打分（Agent Graph 本身不用）。
	// 打分模型复用 cfg.Judge——打分的负载特征（短 prompt、严格 JSON、低 QPS）
	// 与 judge 接近，共享一份配置避免再引入一个 LLM 配置段。
	//
	// stats.enabled=false 时两者都保持 nil；agent/bot 两层都要求 nil-safe，
	// main 这里无需塞哨兵对象。
	var statsStore *stats.Store
	var scoreModel model.BaseChatModel
	if cfg.Stats.Enabled {
		statsStore = stats.NewStore(redisCli, logger)
		sm, err := agent.NewChatModel(ctx, cfg.Judge)
		if err != nil {
			fatal("build stats score model: %v", err)
		}
		scoreModel = sm
		logger.Info("stats feature enabled")
	}

	// Step 6: 构造 Agent Runnable
	runnable, err := agent.Build(ctx, cfg, agent.Deps{
		History: historyRepo,
		Persona: persona,
		Logger:  logger,
		Stats:   statsStore,
	})
	if err != nil {
		fatal("build agent: %v", err)
	}

	// Step 7: 构造 OneBot Adapter
	ad := onebot.New(cfg.Server, cfg.Trigger, logger)
	if err := ad.Start(ctx); err != nil {
		fatal("start adapter: %v", err)
	}

	// Step 8: 跑主循环
	b := bot.New(ad, runnable, statsStore, scoreModel, logger)
	b.Run(ctx) // 阻塞直到 ctx 被取消

	// Step 9: 优雅关闭
	logger.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer shutdownCancel()
	if err := ad.Stop(shutdownCtx); err != nil {
		logger.Error("adapter stop error", slog.Any("err", err))
	}
}

// newLogger 按配置级别构造 slog.Logger（JSON 格式便于采集）。
func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler)
}

// fatal 打印错误到 stderr 并以 1 退出；仅在启动阶段使用。
func fatal(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "fatal: "+format+"\n", args...)
	os.Exit(1)
}
