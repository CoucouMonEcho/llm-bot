// Package agent 的 model.go 负责构造 eino ChatModel 实例。
//
// 目前只实现 OpenAI 兼容协议一种——这覆盖了 OpenAI 官方、DeepSeek、
// Moonshot、OneAPI、Ollama OpenAI Compat 等主流接入方式。
//
// 未来若需要接入 Ark（豆包）、Claude SDK、Gemini 等，新增一个
// 条件分支或拆成 providers/ 子包即可；本文件的对外签名应保持稳定。
package agent

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/model"
	"github.com/echo/llm-bot/internal/config"
)

// NewChatModel 按配置构造一个满足 eino model.BaseChatModel 的 ChatModel。
//
// 参数：
//   - ctx：构造期用的 context（仅做初始化校验，不控制后续 Generate 生命周期）。
//   - cfg：单一 LLM 接入的配置，主模型 / 裁判模型共用同一结构体。
//
// 返回：
//   - model.BaseChatModel：eino 在 Graph 中需要的接口形态。
//   - error：鉴权缺失 / 构造失败会在这里返回。
func NewChatModel(ctx context.Context, cfg config.LLM) (model.BaseChatModel, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("agent: llm api_key is empty (consider OPEN_API_KEY)")
	}

	// 构造底层 HTTP client。设置 Timeout 而不是 ctx deadline 是因为
	// eino 的 Generate/Stream 接收的是调用时的 ctx，与 client timeout
	// 互不冲突；后者是"单次 HTTP 请求的最长等待时间"，前者是"业务取消"。
	httpClient := &http.Client{Timeout: cfg.Timeout()}

	chatModel, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
		BaseURL:    cfg.BaseURL,
		APIKey:     cfg.APIKey,
		Model:      cfg.Model,
		HTTPClient: httpClient,
	})
	if err != nil {
		return nil, fmt.Errorf("agent: build openai chat model: %w", err)
	}
	return chatModel, nil
}
