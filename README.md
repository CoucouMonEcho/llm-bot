# llm-bot

基于 **Go + [eino](https://github.com/cloudwego/eino) Graph + 反向 WebSocket + Redis** 的极简大模型 IM 机器人骨架。

- 首期通过 **OneBot v11 反向 WS** 接入 [NapCatQQ](https://github.com/NapNeko/NapCatQQ)，实现 QQ 消息自动回复。
- 所有 LLM 任务编排以 **eino `compose.Graph`** 呈现；加节点、改流程都只改 Graph。
- 内置 **防提示词注入** 复合节点：正则黑名单 + 并行 LLM 裁判 + 主链 ctx 中断 + 降级回复。
- **Prompt / 人设** 从 YAML 文件加载并在进程内固化，Redis 只负责对话历史。

---

## 架构

```
NapCat ──反向WS──► OneBot Adapter ──► Bot 主循环
                       │                  │
                       └── 源头触发过滤     ▼
                                    ┌──────────────┐
                                    │ Agent Graph  │
                                    │              │
                                    │  guard ──┬── postproc ── saveHistory ──► END
                                    │          │                                 │
                                    │          └── fallback ──────────────────► END
                                    └──────────────┘
                                          ▲
                                          │
                                   Redis (history)
```

`guard` 是复合节点，内部用 `errgroup + context.WithCancel` 并行跑：

```
regex  ── 命中 ─► 直接 Blocked=true
   │ 未命中
   ▼
┌────────────────────────────┐
│  goroutine A: 主链(可取消) │   ──► Reply
│  goroutine B: LLM 裁判     │   ──► 判 attack 则 cancel A
└────────────────────────────┘
```

被判为攻击的消息 **不写入对话历史**；降级回复随机池中挑选，避免被攻击者从固定串识别拦截。

## 目录

```
llm-bot/
├── cmd/bot/main.go                 进程入口
├── configs/
│   ├── config.yaml                 总配置
│   └── prompts/default.yaml        人设 / 护栏 / 用户包装模板
├── internal/
│   ├── config/                     viper 加载 + 校验
│   ├── domain/                     平台无关消息模型
│   ├── adapter/
│   │   ├── adapter.go              Adapter 接口（未来接微信实现此接口）
│   │   └── onebot/                 OneBot v11 反向 WS 实现
│   ├── store/                      Redis 历史存储
│   ├── agent/
│   │   ├── agent.go                Graph 装配
│   │   ├── model.go                OpenAI 兼容 ChatModel
│   │   ├── prompt.go               YAML → Persona 内存固化
│   │   ├── flow/                   节点间共享的 Input / State
│   │   ├── guard/                  防注入复合节点（regex / judge / guard）
│   │   └── nodes/                  postproc / saveHistory / fallback
│   └── bot/                        主循环：Adapter ↔ Agent plumbing
├── Makefile
└── README.md
```

## 快速开始

### 1. 环境

- Go 1.25+
- Redis 7+（只用普通 Redis，不需要 RediSearch / Stack）
- 一个 OpenAI 兼容的 LLM（OpenAI 官方 / DeepSeek / Moonshot / OneAPI 等均可）
- NapCatQQ（或其他 OneBot v11 实现）

### 2. 配置

编辑 `configs/config.yaml`：

```yaml
llm:
  base_url: "https://api.deepseek.com/v1"
  api_key:  "sk-xxx"          # 或通过环境变量 OPEN_API_KEY 覆盖
  model:    "deepseek-chat"

judge:
  base_url: "https://api.deepseek.com/v1"
  api_key:  "sk-xxx"
  model:    "deepseek-chat"   # 建议选便宜的小模型

redis:
  addr: "127.0.0.1:6379"
```

人设在 `configs/prompts/default.yaml` 中调整，修改后需要重启进程。

### 3. 运行

```bash
make build
make run
```

默认监听 `:8080`，WS 路径 `/onebot/v11`。

### 4. NapCat 反向 WS 配置

在 NapCat 的 `onebot11.json` 中开启反向 WebSocket：

```json
{
  "enable": true,
  "host": "127.0.0.1",
  "port": 3001,
  "reverseWs": {
    "enable": true,
    "urls": ["ws://127.0.0.1:8080/onebot/v11"]
  },
  "token": ""
}
```

## 扩展

| 场景 | 改动点 |
| --- | --- |
| 接入微信 | 在 `internal/adapter/` 下新增子包实现 `adapter.Adapter`，`main.go` 再起一个 Adapter 实例即可 |
| 接入新大模型 | `internal/agent/model.go` 中加 switch 分支；若 SDK 复杂可拆 providers 子目录 |
| 加 Agent 节点 | 在 `internal/agent/nodes/` 新增文件，在 `agent.Build` 里 `AddLambdaNode + AddEdge` |
| 加工具调用 (ReAct) | 在 Graph 中用 `AddToolsNode`，把 model 节点与 tools 节点连成循环 |
| RAG 检索 | 在 guard 之后加 retriever 节点，把召回文档注入 prompt |

## License

见 [LICENSE](./LICENSE)。
