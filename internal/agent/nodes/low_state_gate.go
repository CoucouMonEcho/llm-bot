// Package nodes 的 low_state_gate.go 实现"低状态时概率性不回复"的 Lambda 节点。
//
// 位置：loadContext → lowStateGate → buildMessages。
// 本节点只消费 loadContext 已写入 flow.State 的 Affinity / Mood，不再读取 stats.Store。
package nodes

import (
	"context"
	"math"
	"math/rand/v2"

	"github.com/cloudwego/eino/compose"
	"github.com/echo/llm-bot/internal/agent/flow"
)

const (
	lowStateMaxSkipProbability = 0.5

	affinitySkipThreshold = -40
	affinitySkipFloor     = -100

	moodSkipThreshold = -20
	moodSkipFloor     = -50
)

// NewLowStateGate 构造 lowStateGate 节点。
//
// 当 stats 关闭、读取失败或 Affinity/Mood 都为 0 时，loadContext 会留下零值；
// 这种"无信号"状态永远放行。命中跳过时直接返回普通 error，交给 Bot 现有
// Graph 错误处理保持静默不回复。
func NewLowStateGate(rands ...func() float64) *compose.Lambda {
	randFn := rand.Float64
	if len(rands) > 0 && rands[0] != nil {
		randFn = rands[0]
	}

	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		return lowStateGate(st, randFn)
	})
}

func lowStateGate(st *flow.State, randFn func() float64) (*flow.State, error) {
	probability := lowStateSkipProbability(st)
	if probability <= 0 {
		return st, nil
	}
	if randFn() < probability {
		return nil, flow.ErrSkipReply
	}
	return st, nil
}

func lowStateSkipProbability(st *flow.State) float64 {
	if st == nil || (st.Affinity == 0 && st.Mood == 0) {
		return 0
	}
	return math.Max(
		linearSkipProbability(st.Affinity, affinitySkipThreshold, affinitySkipFloor),
		linearSkipProbability(st.Mood, moodSkipThreshold, moodSkipFloor),
	)
}

func linearSkipProbability(value, threshold, floor int) float64 {
	if value >= threshold {
		return 0
	}
	if value <= floor {
		return lowStateMaxSkipProbability
	}
	return float64(threshold-value) / float64(threshold-floor) * lowStateMaxSkipProbability
}
