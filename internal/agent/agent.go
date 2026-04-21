// Package agent 的 agent.go 是 Agent 层的唯一对外入口。
//
// Build 负责：
//  1. 构造主模型 / 裁判模型；
//  2. 编译正则黑名单、构造 Judge；
//  3. 把 guard / postproc / saveHistory / fallback 四个节点装配成
//     compose.Graph 并 Compile 为 Runnable；
//  4. 返回 Runnable 给 Bot 主循环。
//
// 顶层 Graph 形态：
//
//	START ──► guard ──► (branch by Blocked)
//	                     │              │
//	                     │ false        │ true
//	                     ▼              ▼
//	                  postproc      fallback
//	                     │              │
//	                     ▼              ▼
//	                 saveHistory       END
//	                     │
//	                     ▼
//	                    END
package agent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/agent/guard"
	"github.com/echo/llm-bot/internal/agent/nodes"
	"github.com/echo/llm-bot/internal/config"
	"github.com/echo/llm-bot/internal/store"
)

// Deps 聚合构造 Agent 所需的外部依赖。
type Deps struct {
	History store.HistoryRepo
	Persona *Persona
	Logger  *slog.Logger
}

// Runnable 是 Agent 对外暴露的唯一执行形态。
// Bot 主循环调用 Runnable.Invoke(ctx, &flow.Input{...}) 即可。
type Runnable = compose.Runnable[*flow.Input, *flow.State]

// 确保 *Persona 实现 guard.PersonaProvider 接口；编译期断言。
var _ guard.PersonaProvider = (*Persona)(nil)

// 节点 key 常量化，避免字符串散落。
const (
	nodeGuard       = "guard"
	nodePostproc    = "postproc"
	nodeFallback    = "fallback"
	nodeSaveHistory = "saveHistory"
)

// Build 按配置装配 Agent Runnable。
//
// 失败会在启动阶段集中报错——任何一步失败都会让 main 退出进程。
func Build(ctx context.Context, cfg *config.Config, deps Deps) (Runnable, error) {
	// ---- Step 1：构造主模型 / 裁判模型 ----
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
		judge = guard.NewJudge(judgeModel)
	}

	// ---- Step 2：编译正则黑名单 ----
	regex, err := guard.NewRegexMatcher(cfg.Guard.RegexPatterns)
	if err != nil {
		return nil, fmt.Errorf("agent: compile guard regex: %w", err)
	}

	// ---- Step 3：构造各节点 Lambda ----
	guardNode := guard.NewNode(guard.Deps{
		Persona:     deps.Persona,
		History:     deps.History,
		HistorySize: cfg.Agent.HistorySize,
		MainModel:   mainModel,
		Regex:       regex,
		Judge:       judge,
		Logger:      deps.Logger,
	})
	postprocNode := nodes.NewPostproc()
	fallbackNode := nodes.NewFallback(cfg.Guard.FallbackReplies)
	saveHistoryNode := nodes.NewSaveHistory(deps.History, cfg.Agent.HistorySize, deps.Logger)

	// ---- Step 4：装配 Graph ----
	g := compose.NewGraph[*flow.Input, *flow.State]()

	// AddLambdaNode 返回 error 忽略——只有 key 重复等明显错误才会失败，
	// 这里的 key 是常量，不会出问题。
	if err := g.AddLambdaNode(nodeGuard, guardNode); err != nil {
		return nil, fmt.Errorf("agent: add guard node: %w", err)
	}
	if err := g.AddLambdaNode(nodePostproc, postprocNode); err != nil {
		return nil, fmt.Errorf("agent: add postproc node: %w", err)
	}
	if err := g.AddLambdaNode(nodeFallback, fallbackNode); err != nil {
		return nil, fmt.Errorf("agent: add fallback node: %w", err)
	}
	if err := g.AddLambdaNode(nodeSaveHistory, saveHistoryNode); err != nil {
		return nil, fmt.Errorf("agent: add saveHistory node: %w", err)
	}

	// START → guard
	if err := g.AddEdge(compose.START, nodeGuard); err != nil {
		return nil, fmt.Errorf("agent: edge start->guard: %w", err)
	}

	// guard → branch{postproc, fallback}
	branch := compose.NewGraphBranch(
		func(_ context.Context, st *flow.State) (string, error) {
			if st.Blocked {
				return nodeFallback, nil
			}
			return nodePostproc, nil
		},
		map[string]bool{
			nodePostproc: true,
			nodeFallback: true,
		},
	)
	if err := g.AddBranch(nodeGuard, branch); err != nil {
		return nil, fmt.Errorf("agent: add guard branch: %w", err)
	}

	// postproc → saveHistory → END
	if err := g.AddEdge(nodePostproc, nodeSaveHistory); err != nil {
		return nil, fmt.Errorf("agent: edge postproc->saveHistory: %w", err)
	}
	if err := g.AddEdge(nodeSaveHistory, compose.END); err != nil {
		return nil, fmt.Errorf("agent: edge saveHistory->END: %w", err)
	}
	// fallback → END（降级路径不经 saveHistory，攻击消息不入历史）
	if err := g.AddEdge(nodeFallback, compose.END); err != nil {
		return nil, fmt.Errorf("agent: edge fallback->END: %w", err)
	}

	// ---- Step 5：Compile ----
	runnable, err := g.Compile(ctx, compose.WithGraphName("llm-bot-agent"))
	if err != nil {
		return nil, fmt.Errorf("agent: compile graph: %w", err)
	}

	deps.Logger.Info("agent graph compiled",
		slog.String("persona_hash", deps.Persona.SystemPromptHash),
		slog.Bool("judge_enabled", cfg.Guard.JudgeEnabled),
		slog.Int("regex_count", len(cfg.Guard.RegexPatterns)),
		slog.Int("history_size", cfg.Agent.HistorySize))

	return runnable, nil
}

// 编译期断言：当 agent 包被其他包 import 时，如果 eino 升级改了 schema
// 的导出类型会立刻报错而非运行时失败。
var _ *schema.Message = (*schema.Message)(nil)
