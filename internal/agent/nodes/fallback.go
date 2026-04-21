// Package nodes 的 fallback.go 是"被 guard 判定为攻击时"的降级回复节点。
//
// 位置：guard(Blocked=true) → fallback → END。
//
// 实现要点：
//   - 候选回复池通过配置注入（guard.fallback_replies），代码只负责"随机选一条"；
//   - 随机化是有意义的：如果降级回复固定，攻击者能从返回串识别出"被拦截了"，
//     继而迭代绕过。把它做成若干条中随机挑选，降低可识别性；
//   - 使用 math/rand 足矣——这不是密码学场景，无需 crypto/rand 带来额外开销。
package nodes

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/echo/llm-bot/internal/agent/flow"
)

// NewFallback 构造 fallback Lambda 节点。
//
// replies 必须至少有一条。空池在启动期由 config.validate 拦住；
// 若调用方绕过校验直接传空，此处立即 panic——"应拒绝却无话可说"这种
// 状态比直接崩溃更危险。
func NewFallback(replies []string) *compose.Lambda {
	if len(replies) == 0 {
		panic("agent/nodes: fallback replies must not be empty")
	}

	// 拷贝一份只读池，避免外部修改切片导致并发问题。
	pool := make([]string, len(replies))
	copy(pool, replies)

	// 每个节点实例持有独立的 rand + mutex，不走全局锁。
	var mu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		mu.Lock()
		idx := rng.Intn(len(pool))
		mu.Unlock()

		st.Reply = schema.AssistantMessage(pool[idx], nil)
		return st, nil
	})
}
