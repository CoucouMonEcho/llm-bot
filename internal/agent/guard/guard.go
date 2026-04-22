// Package guard 的 guard.go 是防注入复合节点的入口。
//
// 本节点在 Graph 中的位置：紧跟 START，接收 *flow.Input，输出 *flow.State。
// 它把"正则检测 + LLM 裁判（并行）+ 主链 LLM 调用（可被取消）"聚合成
// 一个外观干净的节点。之所以要聚合而不是拆成多个顶层节点，是因为
// eino Graph 的原生 fan-out/fan-in 无法实现"兄弟分支中断"——只有我们
// 自己掌管 goroutine 生命周期时才能做到"裁判判定攻击 → 立即 cancel 主链"。
//
// 内部执行流程（见 Invoke 方法）：
//
//	┌──────────────┐
//	│ 正则同步检测 │──── 命中 ────► 直接返回 Blocked=true (BlockedBy=regex)
//	└──────┬───────┘
//	       │ 未命中
//	       ▼
//	┌──────────────────────────────────────────────┐
//	│  errgroup.WithContext(ctx) ── cancel = ✂    │
//	│  ┌──────────────────┐    ┌────────────────┐ │
//	│  │  main goroutine  │    │ judge goroutine │ │
//	│  │  loadHistory     │    │ Classify(query) │ │
//	│  │  buildMessages   │    │ 若 attack:      │ │
//	│  │  Generate        │    │   cancel() ✂   │ │
//	│  └──────────────────┘    └────────────────┘ │
//	└──────────────────────────────────────────────┘
//	       │
//	       ▼
//	若 verdict==attack → Blocked=true (BlockedBy=judge)
//	否则               → Reply=mainMsg
package guard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
	"github.com/echo/llm-bot/internal/store"
	"golang.org/x/sync/errgroup"
)

// Deps 是 guard 节点构造所需的外部依赖。
// 把依赖显式打包传入而不是全局单例，是为了方便测试与未来 DI。
type Deps struct {
	// Persona 提供 SystemPrompt 与 user_wrapper 模板；只读使用。
	Persona PersonaProvider

	// History 读取对话历史；仅 Load 方法被调用。
	History store.HistoryRepo

	// HistorySize 控制每次请求时从 Redis 读多少条历史。
	HistorySize int

	// MainModel 是主对话模型，生成最终回复。
	MainModel model.BaseChatModel

	// Regex 是预编译好的正则黑名单；nil 等价于"不做正则检查"。
	Regex *RegexMatcher

	// Judge 是 LLM 裁判；nil 表示关闭第二级并行检测。
	Judge *Judge

	// Logger 结构化日志器。
	Logger *slog.Logger
}

// PersonaProvider 抽象出 guard 真正需要从 Persona 里得到的能力，
// 避免 guard 反向依赖 agent 包（会形成导入环）。
// agent.Persona 天然实现了本接口。
type PersonaProvider interface {
	// BuildMessages 组装 system + history + 当前用户消息。
	BuildMessages(history []*schema.Message, query string) ([]*schema.Message, error)
}

// NewNode 构造一个可注册到 compose.Graph 的 Lambda 节点。
//
// 节点签名为 Invoke[*flow.Input, *flow.State]，所以返回 *compose.Lambda。
// 在 agent.go 中通过 g.AddLambdaNode("guard", NewNode(deps)) 注入。
func NewNode(deps Deps) *compose.Lambda {
	g := &guardNode{deps: deps}
	return compose.InvokableLambda(g.Invoke)
}

// guardNode 持有不可变的依赖集合。多 goroutine 并发调用 Invoke 是安全的：
// 所有字段都是只读引用，且 Invoke 内部状态完全在栈上。
type guardNode struct {
	deps Deps
}

// Invoke 是节点的主体逻辑；对应 InvokableLambda 的签名。
func (g *guardNode) Invoke(ctx context.Context, in *flow.Input) (*flow.State, error) {
	st := flow.NewState(in)

	// -----------------------------------------------------------
	// 第 1 步：同步正则检测，命中即短路，完全不调用任何 LLM。
	// -----------------------------------------------------------
	if g.deps.Regex != nil {
		if hit, matched := g.deps.Regex.Match(in.Query); matched {
			st.Blocked = true
			st.BlockedBy = "regex"
			st.HitDetail = hit
			g.deps.Logger.Info("guard: regex blocked",
				slog.String("session", in.SessionID),
				slog.String("pattern", hit))
			return st, nil
		}
	}

	// -----------------------------------------------------------
	// 第 2 步：并行执行 "主链" 与 "LLM 裁判"。
	//
	// 设计要点：
	//  - subCtx 从外层 ctx 派生；defer cancel() 兜底释放。
	//  - 两个 goroutine 共享 subCtx：
	//      * 主链调用 Generate 时把 subCtx 传下去，OpenAI 兼容客户端
	//        会在 ctx.Done() 时断开 HTTP 请求，停止继续消耗 token；
	//      * 裁判一旦判定 attack，立刻主动 cancel 来终止主链。
	//  - 若 Judge 未启用，退化为纯串行：跑完主链返回。
	// -----------------------------------------------------------
	if g.deps.Judge == nil {
		reply, err := g.runMainChain(ctx, in)
		if err != nil {
			return nil, err
		}
		st.Reply = reply
		return st, nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mainReply *schema.Message
		verdict   Verdict = VerdictSafe
	)

	eg, egCtx := errgroup.WithContext(subCtx)

	// -- 主链 goroutine --
	eg.Go(func() error {
		r, err := g.runMainChain(egCtx, in)
		if err != nil {
			// 如果是被主动 cancel（因检测出攻击），归一化为 context.Canceled，
			// 让外层统一识别。
			if errors.Is(err, context.Canceled) {
				return context.Canceled
			}
			return fmt.Errorf("guard: main chain: %w", err)
		}
		mainReply = r
		return nil
	})

	// -- 裁判 goroutine --
	eg.Go(func() error {
		v, err := g.deps.Judge.Classify(egCtx, in.Query)
		if err != nil {
			// 裁判失败（网络/超时）不传递错误，只打日志——
			// 保守地放行，由其他防线兜底。
			if errors.Is(err, context.Canceled) {
				return nil
			}
			g.deps.Logger.Warn("guard: judge error, fail-open",
				slog.String("session", in.SessionID),
				slog.Any("err", err))
			return nil
		}
		verdict = v
		if v == VerdictAttack {
			// 关键一步：立即 cancel，让主链的 HTTP 请求尽快断开。
			cancel()
		}
		return nil
	})

	// 等待两个分支都结束。Wait 返回第一个非 nil 错误；
	// 因此"主链被 cancel → 返回 context.Canceled"是预期内的，需要吞掉。
	if err := eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return nil, err
	}

	// -----------------------------------------------------------
	// 第 3 步：按裁判结论产出 State。
	// -----------------------------------------------------------
	if verdict == VerdictAttack {
		st.Blocked = true
		st.BlockedBy = "judge"
		g.deps.Logger.Info("guard: judge blocked",
			slog.String("session", in.SessionID))
		return st, nil
	}

	// 理论上：verdict==safe 时 mainReply 必非 nil（主链成功返回）。
	// 若 mainReply 为 nil，说明主链在 cancel 未触发的情况下返回了空——
	// 这是错误情况，返回错误让上层走 Bot 的错误路径（不回复 + 日志）。
	if mainReply == nil {
		return nil, fmt.Errorf("guard: main chain returned nil reply without error")
	}
	st.Reply = mainReply
	return st, nil
}

// runMainChain 执行主链的串行三步：
//  1. 从 Redis 读历史；
//  2. 用 Persona 组装 system + history + current 的消息列表；
//  3. 调用主模型 Generate。
//
// 把它抽成私有方法既方便单独复用（Judge 关闭时的串行路径），
// 也让 errgroup 分支函数保持极短的可读性。
func (g *guardNode) runMainChain(ctx context.Context, in *flow.Input) (*schema.Message, error) {
	// Step 1：加载历史。Load 失败不应阻断对话，打 warn 后按"空历史"继续。
	history, err := g.deps.History.Load(ctx, in.SessionID, g.deps.HistorySize)
	if err != nil {
		g.deps.Logger.Warn("guard: load history failed, fallback to empty",
			slog.String("session", in.SessionID),
			slog.Any("err", err))
		history = nil
	}

	// Step 2：组装消息。人设已经在 Persona 内存中，不会 IO。
	messages, err := g.deps.Persona.BuildMessages(history, in.Query)
	if err != nil {
		return nil, err
	}

	// Step 3：调用主模型。Generate 在 ctx 取消时会尽快断开 HTTP 请求。
	reply, err := g.deps.MainModel.Generate(ctx, messages)
	if err != nil {
		return nil, err
	}
	return reply, nil
}
