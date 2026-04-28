// Package nodes 的 fallback.go 是"被 guard 判定为攻击时"的降级回复节点。
//
// 位置：guard(Verdict.Blocked) → fallback → scoreStats → END。
//
// 实现要点：
//   - 候选回复池通过配置注入（guard.fallback_replies），代码只负责"随机选一条"；
//   - 随机化是有意义的：如果降级回复固定，攻击者能从返回串识别出"被拦截了"，
//     继而迭代绕过。把它做成若干条中随机挑选，降低可识别性；
//   - 直接用标准库 rand：这不是密码学场景，无需 crypto/rand 带来额外开销；
//     math/rand/v2（Go 1.22+）的顶层函数是并发安全且自动 seed 的，
//     因此无需手动持有 rng 实例或 mutex。
package nodes

import (
	"context"
	"math/rand/v2"

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

	// replies 源自配置，启动后不再改动；字符串不可变，直接引用即可。
	return compose.InvokableLambda(func(_ context.Context, st *flow.State) (*flow.State, error) {
		st.Reply = schema.AssistantMessage(replies[rand.IntN(len(replies))], nil)
		return st, nil
	})
}
