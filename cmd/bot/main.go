// Command bot 是 llm-bot 的进程入口。
//
// 职责（按执行顺序）：
//  1. 解析命令行 flag 找到 config.yaml；
//  2. 加载配置、构造 logger；
//  3. 连 Redis，构造 HistoryRepo；
//  4. 加载人设 YAML，构造 Persona；
//  5. 构造 stats 资源（若启用）：stats.Store 与打分 ChatModel；
//  6. 构造 Agent Runnable（这一步会构造主/裁判 ChatModel、编译 Graph）；
//  7. 构造主动消息资源（若启用）；
//  8. 构造 OneBot Adapter；
//  9. 启动主动调度并把 Bot 主循环跑起来；
//  10. 监听 SIGINT/SIGTERM 做优雅关闭。
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
	"github.com/echo/llm-bot/internal/proactive"
	"github.com/echo/llm-bot/internal/stats"
	"github.com/echo/llm-bot/internal/store"
)

// defaultShutdownTimeout 是优雅关闭阶段最多等待的时长。
// 超过这个时间仍有未完成请求就放弃它们，保证进程能退出。
const defaultShutdownTimeout = 10 * time.Second

func main() {
	// 步骤 1：解析启动参数。
	configPath := flag.String("config", "configs/config.yaml", "path to config yaml")
	flag.Parse()

	// 步骤 2：加载配置与日志器。
	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal("load config: %v", err)
	}
	logger := newLogger(cfg.Log.Level)
	logger.Info("config loaded",
		slog.String("path", *configPath),
		slog.String("model", cfg.LLM.Model),
		slog.String("judge_model", cfg.Judge.Model))

	// 步骤 3：连接 Redis，后续历史、stats、主动消息状态都复用同一个客户端。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisCli, err := store.NewRedisClient(ctx, cfg.Redis)
	if err != nil {
		fatal("connect redis: %v", err)
	}
	defer func() { _ = redisCli.Close() }()
	historyRepo := store.NewHistoryRepo(redisCli)

	// 步骤 4：加载人设模板。
	persona, err := agent.LoadPersona(cfg.Agent.PromptFile)
	if err != nil {
		fatal("load persona: %v", err)
	}

	// 步骤 5：构造 stats 资源（若启用）。
	// 产出两件东西:
	//   - statsStore：传给 agent.Build，用于 prepareStats 节点结算并读取参数快照；
	//   - scoreModel：传给 agent.Build，用于 scoreStats 节点在回复生成后异步打分。
	// 打分模型复用 cfg.Judge——打分的负载特征（短 prompt、严格 JSON、低 QPS）
	// 与 judge 接近，共享一份配置避免再引入一个 LLM 配置段。
	//
	// stats.enabled=false 时两者都保持 nil；agent graph 内部会安全处理 nil，
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

	// 步骤 6：构造 Agent Runnable。
	runnable, err := agent.Build(ctx, cfg, agent.Deps{
		History:    historyRepo,
		Persona:    persona,
		Logger:     logger,
		Stats:      statsStore,
		ScoreModel: scoreModel,
	})
	if err != nil {
		fatal("build agent: %v", err)
	}

	// 步骤 7：构造主动消息组件（若启用）。
	// Bot 层只拿到 ActivityRecorder 这个窄接口，用来记录真实入站活跃；
	// 主动消息的候选选择、去重、冷却、dry-run 与 Redis key 都留在 proactive 包内。
	// 这里先把 State / Selector / Generator 拼好，Scheduler 等 Adapter start 后再启动，
	// 因为它需要把生成结果通过同一个 Adapter 发回平台。
	var activityRecorder bot.ActivityRecorder
	var proactiveState *proactive.State
	var proactiveSelector *proactive.Selector
	var proactiveGenerator *proactive.Generator
	var proactiveCfg proactive.Config
	if cfg.Proactive.Enabled {
		proactiveState = proactive.NewState(redisCli, logger)
		activityRecorder = proactive.NewActivityRecorder(proactiveState, logger, proactive.RecorderConfig{
			RecentGroupEventCap: cfg.Proactive.RecentEventsCap,
		})

		proactiveModel, err := agent.NewChatModel(ctx, cfg.LLM)
		if err != nil {
			fatal("build proactive model: %v", err)
		}
		proactiveCfg = proactive.Config{
			Enabled:     cfg.Proactive.Enabled,
			WindowStart: cfg.Proactive.WindowStart,
			WindowEnd:   cfg.Proactive.WindowEnd,
			Interval:    cfg.Proactive.Interval(),
			Jitter:      cfg.Proactive.JitterMax(),
			DailyLimit:  cfg.Proactive.DailyLimit,
			DryRun:      cfg.Proactive.DryRun,
			PendingTTL:  cfg.Proactive.PendingTTL(),
			Selector: proactive.SelectorConfig{
				AffinityTopN:    cfg.Proactive.TopN,
				MinSince:        cfg.Proactive.MinSinceLastInbound(),
				MaxSince:        cfg.Proactive.MaxSinceLastInbound(),
				RecentEventScan: cfg.Proactive.RecentEventsCap,
				SessionCooldown: cfg.Proactive.SessionCooldown(),
			},
		}
		proactiveSelector = proactive.NewSelector(proactiveState, logger, proactiveCfg.Selector)
		proactiveGenerator = proactive.NewGenerator(proactive.GeneratorOptions{
			Model:   proactiveModel,
			History: historyRepo,
			Logger:  logger,
			Config:  proactiveCfg.Generator,
		})
		logger.Info("proactive feature enabled", slog.Bool("dry_run", cfg.Proactive.DryRun))
	}

	// 步骤 8：构造 OneBot Adapter。
	ad := onebot.New(cfg.Server, cfg.Trigger, logger)
	if err := ad.Start(ctx); err != nil {
		fatal("start adapter: %v", err)
	}
	if proactiveState != nil {
		// 主动调度与主循环共享同一个 ctx：进程收到退出信号时两条链路一起停。
		// Scheduler 内部会做时间窗、全局开关、每日上限和候选锁定；main 只负责接线。
		proactiveScheduler := proactive.NewScheduler(proactive.Options{
			State:     proactiveState,
			Selector:  proactiveSelector,
			Generator: proactiveGenerator,
			Sender:    ad,
			Logger:    logger,
			Config:    proactiveCfg,
		})
		go proactiveScheduler.Run(ctx)
	}

	// 步骤 9：运行主循环。
	b := bot.New(ad, runnable, activityRecorder, logger)
	b.Run(ctx) // 阻塞直到 ctx 被取消

	// 步骤 10：优雅关闭。
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
