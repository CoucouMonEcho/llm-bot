// Package agent 的 agent.go 是 Agent 层的唯一对外入口。
//
// Build 负责：
//  1. 构造主模型 / 裁判模型；
//  2. 编译正则黑名单、构造 Judge；
//  3. 把 regexGate / prepareStats / loadHistory / buildMessages / guardedModel /
//     postproc / saveHistory / fallback / scoreStats 九个节点装配成 compose.Graph
//     并编译为 Runnable；
//  4. 返回 Runnable 给 Bot 主循环。
//
// 顶层 Graph 形态：
//
//	START
//	  │
//	  ▼
//	regexGate ── (命中) ──► fallback ────────────────┐
//	  │ (未命中)
//	  ▼
//	prepareStats ──► loadHistory ──► buildMessages ──► guardedModel
//	                                                   │
//	                                      攻击 ────────┴──────── 放行
//	                                      │                        │
//	                                      ▼                        ▼
//	                                  fallback                  postproc
//	                                      │                        │
//	                                      └──────────┐             ▼
//	                                                 │        saveHistory
//	                                                 │             │
//	                                                 ▼             ▼
//	                                             scoreStats ◄──────┘
//	                                                 │
//	                                                 ▼
//	                                                END
package agent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/agent/guard"
	"github.com/echo/llm-bot/internal/agent/nodes"
	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/stats"
	"github.com/echo/llm-bot/internal/store"
)

// Deps 聚合构造 Agent 所需的外部依赖。
type Deps struct {
	History store.HistoryRepo
	Persona *Persona
	Logger  *slog.Logger
	// Stats 可为 nil，表示关闭人设参数调制（好感度、心情与未来扩展参数）。
	// 节点内部负责跳过 nil，这样 main 不需要伪造一个空 Store。
	Stats *stats.Store
	// JudgePrompt 是裁判模型的 system prompt；仅在 guard.judge_enabled=true 时使用。
	JudgePrompt string
	// ScoreModel 可为 nil，表示不触发回复后的 stats 异步打分。
	// 允许单独传入模型是为了让打分负载与主回复模型解耦，当前 main 复用 Judge 配置。
	ScoreModel model.BaseChatModel
	// ScorePrompt 是 stats 打分模型的 system prompt；仅在 ScoreModel 非 nil 时使用。
	ScorePrompt string
}

// Runnable 是 Agent 对外暴露的唯一执行形态。
// Bot 主循环调用 Runnable.Invoke(ctx, &flow.Input{...}) 即可。
type Runnable = compose.Runnable[*flow.Input, *flow.State]

// 节点 key 常量化，避免字符串散落。
const (
	nodeRegexGate     = "regexGate"
	nodePrepareStats  = "prepareStats"
	nodeLoadHistory   = "loadHistory"
	nodeBuildMessages = "buildMessages"
	nodeGuardedModel  = "guardedModel"
	nodePostproc      = "postproc"
	nodeFallback      = "fallback"
	nodeSaveHistory   = "saveHistory"
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

	var judge *guard.Judge
	if cfg.Guard.JudgeEnabled {
		judgeModel, err := NewChatModel(ctx, cfg.Judge)
		if err != nil {
			return nil, fmt.Errorf("agent: new judge chat model: %w", err)
		}
		judge = guard.NewJudge(judgeModel, deps.JudgePrompt)
	}

	// 步骤 2：编译正则黑名单。
	regex, err := guard.NewRegexMatcher(cfg.Guard.RegexPatterns)
	if err != nil {
		return nil, fmt.Errorf("agent: compile guard regex: %w", err)
	}

	// 步骤 3：构造各节点 Lambda。
	// buildMessages 节点用函数字面量注入 Persona.BuildMessages，避免 nodes
	// 反向依赖 agent 包（会形成 import 环）。
	regexGateNode := guard.NewRegexGate(regex, deps.Logger)
	prepareStatsNode := nodes.NewPrepareStats(deps.Stats)
	loadHistoryNode := nodes.NewLoadHistory(deps.History, cfg.Agent.HistorySize, deps.Logger)
	buildMessagesNode := nodes.NewBuildMessages(deps.Persona.BuildMessages)
	guardedModelNode := guard.NewGuardedModel(mainModel, judge, deps.Logger)
	postprocNode := nodes.NewPostproc(cfg.Agent.EmptyReplyFallback)
	fallbackNode := nodes.NewFallback(cfg.Guard.FallbackReplies)
	saveHistoryNode := nodes.NewSaveHistory(deps.History, cfg.Agent.HistorySize, deps.Logger)
	scoreStatsNode := nodes.NewScoreStats(deps.Stats, deps.ScoreModel, deps.ScorePrompt, deps.Logger)

	// 步骤 4：装配 Graph。
	g := compose.NewGraph[*flow.Input, *flow.State]()

	for _, add := range []struct {
		key    string
		lambda *compose.Lambda
	}{
		{nodeRegexGate, regexGateNode},
		{nodePrepareStats, prepareStatsNode},
		{nodeLoadHistory, loadHistoryNode},
		{nodeBuildMessages, buildMessagesNode},
		{nodeGuardedModel, guardedModelNode},
		{nodePostproc, postprocNode},
		{nodeFallback, fallbackNode},
		{nodeSaveHistory, saveHistoryNode},
		{nodeScoreStats, scoreStatsNode},
	} {
		if err := g.AddLambdaNode(add.key, add.lambda); err != nil {
			return nil, fmt.Errorf("agent: add %s node: %w", add.key, err)
		}
	}

	// 线性边一次性声明：两条 branch 因各自分支不同仍需单独装配。
	// fallback → scoreStats 而不是 saveHistory，表明"降级路径不经 saveHistory，
	// 攻击消息不入历史但仍触发回复后打分"是 graph 的显式结构而非副作用。
	edges := []struct{ from, to string }{
		{compose.START, nodeRegexGate},
		{nodePrepareStats, nodeLoadHistory},
		{nodeLoadHistory, nodeBuildMessages},
		{nodeBuildMessages, nodeGuardedModel},
		{nodePostproc, nodeSaveHistory},
		{nodeSaveHistory, nodeScoreStats},
		{nodeFallback, nodeScoreStats},
		{nodeScoreStats, compose.END},
	}
	for _, e := range edges {
		if err := g.AddEdge(e.from, e.to); err != nil {
			return nil, fmt.Errorf("agent: edge %s->%s: %w", e.from, e.to, err)
		}
	}

	// regexGate 根据同步正则结果路由：放行进入 prepareStats，命中进入 fallback。
	regexBranch := compose.NewGraphBranch(
		func(_ context.Context, st *flow.State) (string, error) {
			if st.Verdict.Blocked() {
				return nodeFallback, nil
			}
			return nodePrepareStats, nil
		},
		map[string]bool{
			nodePrepareStats: true,
			nodeFallback:     true,
		},
	)
	if err := g.AddBranch(nodeRegexGate, regexBranch); err != nil {
		return nil, fmt.Errorf("agent: add regexGate branch: %w", err)
	}

	// guardedModel 根据裁判结果路由：放行进入 postproc，攻击进入 fallback。
	verdictBranch := compose.NewGraphBranch(
		func(_ context.Context, st *flow.State) (string, error) {
			if st.Verdict.Blocked() {
				return nodeFallback, nil
			}
			return nodePostproc, nil
		},
		map[string]bool{
			nodePostproc: true,
			nodeFallback: true,
		},
	)
	if err := g.AddBranch(nodeGuardedModel, verdictBranch); err != nil {
		return nil, fmt.Errorf("agent: add guardedModel branch: %w", err)
	}

	// 步骤 5：编译 Graph。
	runnable, err := g.Compile(ctx, compose.WithGraphName("llm-bot-agent"))
	if err != nil {
		return nil, fmt.Errorf("agent: compile graph: %w", err)
	}

	deps.Logger.Info("agent graph compiled",
		slog.Bool("judge_enabled", cfg.Guard.JudgeEnabled),
		slog.Int("regex_count", len(cfg.Guard.RegexPatterns)),
		slog.Int("history_size", cfg.Agent.HistorySize),
		slog.Bool("stats_enabled", deps.Stats != nil),
		slog.Bool("stats_scoring_enabled", deps.Stats != nil && deps.ScoreModel != nil))

	return runnable, nil
}
