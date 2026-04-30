// Package agent 的 agent.go 是 Agent 层的唯一对外入口。
//
// Build 负责：
//  1. 构造主模型 / 裁判模型；
//  2. 构造 judgeGate / loadContext / lowStateGate / buildMessages /
//     chatModel / postproc / saveHistory / updateMemory / scoreStats 九个节点
//     装配成 compose.Graph
//     并编译为 Runnable；
//  3. 返回 Runnable 给 Bot 主循环。
//
// 顶层 Graph 形态：
//
//	START
//	  │
//	  ▼
//	judgeGate
//	  │ (safe；非 safe 直接静默不回复)
//	  ▼
//	loadContext ──► lowStateGate ──► buildMessages ──► chatModel ──► postproc
//	                                                   │
//	                                                   ▼
//	                                              saveHistory
//	                                                   │
//	                                                   ▼
//	                                              updateMemory
//	                                                   │
//	                                                   ▼
//	                                             scoreStats
//	                                                   │
//	                                                   ▼
//	                                                  END
package agent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/agent/nodes"
	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/infra/store"
	"github.com/echo/llm-bot/internal/memory"
	"github.com/echo/llm-bot/internal/stats"
)

// Deps 聚合构造 Agent 所需的外部依赖。
type Deps struct {
	History store.HistoryRepo
	Persona *Persona
	Logger  *slog.Logger
	// Stats 可为 nil，表示关闭人设参数调制（好感度、心情与未来扩展参数）。
	// 节点内部负责跳过 nil，这样 main 不需要伪造一个空 Store。
	Stats *stats.Store
	// Memory 可为 nil，表示关闭长期用户记忆注入与更新。
	Memory *memory.Store
	// JudgePrompt 是裁判模型的 system prompt。删掉正则层后，裁判固定作为输入侧防线。
	JudgePrompt string
	// ScoreModel 可为 nil，表示不触发回复后的 stats 异步打分。
	// 允许单独传入模型是为了让打分负载与主回复模型解耦，当前 main 复用 Judge 配置。
	ScoreModel model.BaseChatModel
	// ScorePrompt 是 stats 打分模型的 system prompt；仅在 ScoreModel 非 nil 时使用。
	ScorePrompt string
	// MemoryModel 可为 nil，表示不触发回复后的长期记忆异步更新。
	MemoryModel model.BaseChatModel
	// MemoryPrompt 是长期记忆更新模型的 system prompt；仅在 MemoryModel 非 nil 时使用。
	MemoryPrompt string
	// GroupBuffer 可为 nil，表示关闭群聊短期上下文背景注入。
	// 节点内部负责跳过 nil。
	GroupBuffer store.GroupBufferRepo
}

// Runnable 是 Agent 对外暴露的唯一执行形态。
// Bot 主循环调用 Runnable.Invoke(ctx, &flow.Input{...}) 即可。
type Runnable = compose.Runnable[*flow.Input, *flow.State]

// 节点 key 常量化，避免字符串散落。
const (
	nodeJudgeGate     = "judgeGate"
	nodeLoadContext   = "loadContext"
	nodeLowStateGate  = "lowStateGate"
	nodeBuildMessages = "buildMessages"
	nodeChatModel     = "chatModel"
	nodePostproc      = "postproc"
	nodeSaveHistory   = "saveHistory"
	nodeUpdateMemory  = "updateMemory"
	nodeScoreStats    = "scoreStats"
)

// Build 按配置装配 Agent Runnable。
//
// 失败会在启动阶段集中报错——任何一步失败都会让 main 退出进程。
func Build(ctx context.Context, cfg *config.Config, deps Deps) (Runnable, error) {
	// 步骤 1：构造主模型与裁判模型。
	mainModel, err := NewChatModel(ctx, cfg.LLM)
	if err != nil {
		return nil, fmt.Errorf("agent: new main chat model: %w", err)
	}

	judgeModel, err := NewChatModel(ctx, cfg.Judge)
	if err != nil {
		return nil, fmt.Errorf("agent: new judge chat model: %w", err)
	}

	// 步骤 2：构造各节点 Lambda。
	// buildMessages 节点用函数字面量注入 Persona.BuildMessages，避免 nodes
	// 反向依赖 agent 包（会形成 import 环）。
	judgeGateNode := nodes.NewJudgeGate(judgeModel, deps.JudgePrompt, deps.Logger)
	loadContextNode := nodes.NewLoadContext(deps.Stats, deps.Memory, deps.History, deps.GroupBuffer, cfg.Memory.MaxChars, cfg.Agent.HistorySize, deps.Logger)
	lowStateGateNode := nodes.NewLowStateGate()
	buildMessagesNode := nodes.NewBuildMessages(deps.Persona.BuildMessages)
	chatModelNode := nodes.NewChatModel(mainModel)
	postprocNode := nodes.NewPostproc(cfg.Agent.EmptyReplyFallback)
	saveHistoryNode := nodes.NewSaveHistory(deps.History, cfg.Agent.HistorySize, deps.Logger)
	updateMemoryNode := nodes.NewUpdateMemory(deps.Memory, deps.MemoryModel, deps.MemoryPrompt, cfg.Memory.MaxChars, deps.Logger)
	scoreStatsNode := nodes.NewScoreStats(deps.Stats, deps.ScoreModel, deps.ScorePrompt, deps.Logger)

	// 步骤 3：装配 Graph。
	g := compose.NewGraph[*flow.Input, *flow.State]()

	for _, add := range []struct {
		key    string
		lambda *compose.Lambda
	}{
		{nodeJudgeGate, judgeGateNode},
		{nodeLoadContext, loadContextNode},
		{nodeLowStateGate, lowStateGateNode},
		{nodeBuildMessages, buildMessagesNode},
		{nodeChatModel, chatModelNode},
		{nodePostproc, postprocNode},
		{nodeSaveHistory, saveHistoryNode},
		{nodeUpdateMemory, updateMemoryNode},
		{nodeScoreStats, scoreStatsNode},
	} {
		if err := g.AddLambdaNode(add.key, add.lambda); err != nil {
			return nil, fmt.Errorf("agent: add %s node: %w", add.key, err)
		}
	}

	// judgeGate / lowStateGate 命中不回复时直接返回 flow.ErrSkipReply，
	// Graph 中断后 Bot 维持静默；这类消息不入历史、不触发回复后打分。
	edges := []struct{ from, to string }{
		{compose.START, nodeJudgeGate},
		{nodeJudgeGate, nodeLoadContext},
		{nodeLoadContext, nodeLowStateGate},
		{nodeLowStateGate, nodeBuildMessages},
		{nodeBuildMessages, nodeChatModel},
		{nodeChatModel, nodePostproc},
		{nodePostproc, nodeSaveHistory},
		{nodeSaveHistory, nodeUpdateMemory},
		{nodeUpdateMemory, nodeScoreStats},
		{nodeScoreStats, compose.END},
	}
	for _, e := range edges {
		if err := g.AddEdge(e.from, e.to); err != nil {
			return nil, fmt.Errorf("agent: edge %s->%s: %w", e.from, e.to, err)
		}
	}

	// 步骤 4：编译 Graph。
	runnable, err := g.Compile(ctx, compose.WithGraphName("llm-bot-agent"))
	if err != nil {
		return nil, fmt.Errorf("agent: compile graph: %w", err)
	}

	deps.Logger.Info("agent graph compiled",
		slog.String("main_path", "judgeGate->loadContext->lowStateGate->buildMessages->chatModel->postproc->saveHistory->updateMemory->scoreStats"),
		slog.String("blocked_path", "skipReply"),
		slog.Int("history_size", cfg.Agent.HistorySize),
		slog.Bool("stats_enabled", deps.Stats != nil),
		slog.Bool("stats_scoring_enabled", deps.Stats != nil && deps.ScoreModel != nil),
		slog.Bool("memory_enabled", deps.Memory != nil),
		slog.Bool("memory_update_enabled", deps.Memory != nil && deps.MemoryModel != nil),
		slog.Bool("group_buffer_enabled", deps.GroupBuffer != nil))

	return runnable, nil
}
