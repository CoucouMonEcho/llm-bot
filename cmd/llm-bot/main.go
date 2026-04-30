// Command bot 是 llm-bot 的进程入口。
//
// 职责（按执行顺序）：
//  1. 解析命令行 flag 找到 config.yaml；
//  2. 加载配置、构造 logger；
//  3. 连 Redis，构造 HistoryRepo；
//  4. 加载人设与裁判 prompt；
//  5. 必要时构造 stats / memory 共用的 judge ChatModel；
//  6. 构造 stats / memory 资源（若启用）：Store、prompt 与异步更新 ChatModel；
//  7. 构造 Agent Runnable（这一步会构造主/裁判 ChatModel、编译 Graph）；
//  8. 构造 OneBot Adapter；
//  9. 构造主动消息资源（始终构造，运行期由 Redis 开关控制）；
//  10. 启动主动调度并把 Bot 主循环跑起来；
//  11. 监听 SIGINT/SIGTERM 做优雅关闭。
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
	"github.com/echo/llm-bot/internal/agent"
	"github.com/echo/llm-bot/internal/app/bot"
	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/infra/store"
	"github.com/echo/llm-bot/internal/llmtext"
	"github.com/echo/llm-bot/internal/memory"
	"github.com/echo/llm-bot/internal/platform/adapter/onebot"
	"github.com/echo/llm-bot/internal/proactive"
	"github.com/echo/llm-bot/internal/stats"
	"github.com/redis/go-redis/v9"
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

	// 群聊短期上下文缓存：仅用于 @bot 时回溯刚才群里在聊什么。
	// Enabled=false 时为 nil，Bot / Agent 内部都按"功能关闭"处理。
	var groupBuffer store.GroupBufferRepo
	if cfg.GroupBuffer.Enabled {
		groupBuffer = store.NewGroupBufferRepo(redisCli, cfg.GroupBuffer.MaxMessages, cfg.GroupBuffer.TTL())
		logger.Info("group buffer enabled",
			slog.Int("max_messages", cfg.GroupBuffer.MaxMessages),
			slog.Int("ttl_sec", cfg.GroupBuffer.TTLSec))
	}

	// 步骤 4：加载人设模板。
	persona, err := agent.LoadPersona(cfg.Agent.PromptFile)
	if err != nil {
		fatal("load persona: %v", err)
	}

	judgePrompt, err := llmtext.LoadPromptFile(cfg.Guard.JudgePromptFile, "guard")
	if err != nil {
		fatal("load judge prompt: %v", err)
	}

	// 步骤 5：stats / memory 都用 cfg.Judge 做异步打分 / 摘要更新；只有任一启用时
	// 才构造一次共享的 judge ChatModel，避免没用上还白白连一次模型 API。
	var judgeModel model.BaseChatModel
	if cfg.Stats.Enabled || cfg.Memory.Enabled {
		judgeModel, err = agent.NewChatModel(ctx, cfg.Judge)
		if err != nil {
			fatal("build judge chat model: %v", err)
		}
	}

	// 步骤 6：构造 stats / memory 资源（若启用）。
	// stats 产出三件东西:
	//   - statsStore：传给 agent.Build，用于 prepareStats 节点结算并读取参数快照；
	//   - scorePrompt：传给 scoreStats 节点作为打分模型的 system prompt；
	//   - scoreModel：传给 agent.Build，用于 scoreStats 节点在回复生成后异步打分。
	// memory 同理传入 Store / prompt / model，用于读取上下文与回复后的异步摘要更新。
	// 两条异步链路都复用 cfg.Judge——负载特征（短 prompt、严格 JSON、低 QPS）
	// 与 judge 接近，共享一份配置避免再引入额外 LLM 配置段。
	//
	// 功能关闭时这些资源都保持零值；agent graph 内部会安全处理 nil，main 这里
	// 无需塞哨兵对象。
	statsStore, scoreModel, scorePrompt, err := buildStats(cfg, redisCli, logger, judgeModel)
	if err != nil {
		fatal("%v", err)
	}
	memoryStore, memoryModel, memoryPrompt, err := buildMemory(cfg, redisCli, logger, judgeModel)
	if err != nil {
		fatal("%v", err)
	}

	// 步骤 7：构造 Agent Runnable。
	runnable, err := agent.Build(ctx, cfg, agent.Deps{
		History:      historyRepo,
		Persona:      persona,
		Logger:       logger,
		Stats:        statsStore,
		Memory:       memoryStore,
		GroupBuffer:  groupBuffer,
		JudgePrompt:  judgePrompt,
		ScoreModel:   scoreModel,
		ScorePrompt:  scorePrompt,
		MemoryModel:  memoryModel,
		MemoryPrompt: memoryPrompt,
	})
	if err != nil {
		fatal("build agent: %v", err)
	}

	// 步骤 8：构造 OneBot Adapter。
	ad := onebot.New(cfg.Server, cfg.Trigger, cfg.Blacklist, logger)
	if err := ad.Start(ctx); err != nil {
		fatal("start adapter: %v", err)
	}

	// 步骤 9：构造主动消息组件。
	// 主动消息组件始终构造，运行期由 Redis bot_proactive_enabled 控制是否真发；
	// Bot 层在群消息发送成功后记录 bot 开口时间，群冷却阈值、时间窗与 Redis
	// key 都留在 proactive 包内。
	// Scheduler 在 Adapter start 之后再启动，因为它需要把生成结果通过同一个
	// Adapter 发回平台。
	proactiveState := proactive.NewState(redisCli, logger)
	b := bot.New(ad, runnable, proactiveState, groupBuffer, logger, cfg.Trigger)
	proactiveScheduler, err := buildProactiveScheduler(ctx, cfg, proactiveState, historyRepo, ad, b.OpenProactiveFollowup, logger)
	if err != nil {
		fatal("%v", err)
	}

	// 步骤 10：启动主动调度并跑主循环。
	// 主动调度与主循环共享同一个 ctx：进程收到退出信号时两条链路一起停。
	go proactiveScheduler.Run(ctx)
	b.Run(ctx) // 阻塞直到 ctx 被取消

	// 步骤 11：优雅关闭。
	logger.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer shutdownCancel()
	if err := ad.Stop(shutdownCtx); err != nil {
		logger.Error("adapter stop error", slog.Any("err", err))
	}
}

// buildStats 构造 stats 模块所需的 Store / 打分模型 / 打分 prompt。
//
// 关闭时返回零值，调用方按零值传给 agent.Build；judgeModel 此时也无需构造。
// 这里把"装配"集中到一处，让 main 不再重复 if cfg.Stats.Enabled 的分支。
func buildStats(cfg *config.Config, redisCli *redis.Client, logger *slog.Logger, judgeModel model.BaseChatModel) (*stats.Store, model.BaseChatModel, string, error) {
	if !cfg.Stats.Enabled {
		return nil, nil, "", nil
	}
	scorePrompt, err := stats.LoadScorePrompt(cfg.Stats.ScorePromptFile)
	if err != nil {
		return nil, nil, "", fmt.Errorf("load stats score prompt: %w", err)
	}
	statsStore := stats.NewStore(redisCli, logger)
	logger.Info("stats feature enabled")
	return statsStore, judgeModel, scorePrompt, nil
}

// buildMemory 构造长期记忆所需的 Store / 更新模型 / 更新 prompt。
//
// 关闭时返回零值。和 buildStats 一样，靠共享的 judgeModel 复用 cfg.Judge。
func buildMemory(cfg *config.Config, redisCli *redis.Client, logger *slog.Logger, judgeModel model.BaseChatModel) (*memory.Store, model.BaseChatModel, string, error) {
	if !cfg.Memory.Enabled {
		return nil, nil, "", nil
	}
	updatePrompt, err := memory.LoadUpdatePrompt(cfg.Memory.UpdatePromptFile)
	if err != nil {
		return nil, nil, "", fmt.Errorf("load memory update prompt: %w", err)
	}
	memoryStore := memory.NewStore(redisCli, logger)
	logger.Info("memory feature enabled", slog.Int("max_chars", cfg.Memory.MaxChars))
	return memoryStore, judgeModel, updatePrompt, nil
}

// buildProactiveScheduler 构造主动消息调度器。
//
// Scheduler 始终构造（永不返回 nil），运行期是否真正发送由 Redis 上的
// `bot_proactive_enabled` 控制，未设值默认关闭。Sender 作为参数传入，是因为
// 只有 Adapter 自己知道怎么把消息发回平台，proactive 内部不绑定 onebot 实现。
// historyRepo 传给 Generator 用来读取语气参考，也传给 Scheduler 用来在主动消息
// 发送成功后写回群历史。onSendSuccess 由 Bot 注入，用来打开主动群发后的首个
// 普通回应窗口。
func buildProactiveScheduler(ctx context.Context, cfg *config.Config, state *proactive.State, historyRepo store.HistoryRepo, sender proactive.Sender, onSendSuccess func(string, time.Time), logger *slog.Logger) (*proactive.Scheduler, error) {
	proactiveModel, err := agent.NewChatModel(ctx, cfg.LLM)
	if err != nil {
		return nil, fmt.Errorf("build proactive model: %w", err)
	}
	prompts, err := proactive.LoadGeneratorPrompts(cfg.Proactive.PromptFile)
	if err != nil {
		return nil, fmt.Errorf("load proactive prompts: %w", err)
	}
	// HistorySize / MaxHistoryChars 直接在装配点固化：proactive 包内不再保留
	// 默认值常量，main 是唯一选择"取多少历史给主动开场白参考"的地方。
	proactiveCfg := proactive.Config{
		WindowStart:     cfg.Proactive.WindowStart,
		WindowEnd:       cfg.Proactive.WindowEnd,
		Interval:        cfg.Proactive.Interval(),
		Jitter:          cfg.Proactive.JitterMax(),
		BotSilence:      cfg.Proactive.BotSilenceThreshold(),
		HistorySize:     6,
		MaxHistoryChars: 1200,
	}
	generator := proactive.NewGenerator(proactive.GeneratorOptions{
		Model:   proactiveModel,
		History: historyRepo,
		Logger:  logger,
		Config:  proactiveCfg,
		Prompts: prompts,
	})
	scheduler := proactive.NewScheduler(proactive.Options{
		State:         state,
		Generator:     generator,
		Sender:        sender,
		History:       historyRepo,
		HistoryMax:    cfg.Agent.HistorySize,
		Logger:        logger,
		Config:        proactiveCfg,
		OnSendSuccess: onSendSuccess,
	})
	logger.Info("proactive scheduler started")
	return scheduler, nil
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
