# llm-bot

基于 **Go + [eino](https://github.com/cloudwego/eino) Graph + 反向 WebSocket + Redis** 的极简大模型 IM 机器人骨架，有效防止prompt注入。

- 通过 **OneBot v11 反向 WS** 对接 [NapCatQQ](https://github.com/NapNeko/NapCatQQ)，实现 QQ 消息自动回复。
- LLM 任务编排用 **eino `compose.Graph`** 表达，加节点、改流程只改 Graph。
- 内置双重**防提示词注入**：同步正则黑名单 + 并行 LLM 裁判（判为攻击时立即取消主链）+ 随机降级回复。
- 人设（Persona）从 YAML 加载，进程启动后固化为内存只读快照。
- Redis 仅用于对话历史。

---

## 架构

```
NapCat ──反向 WS──► OneBot Adapter ──► Bot 主循环
                          │                │
                          └── 触发过滤 ────┐▼
                                    ┌──── Agent Graph ────┐
                                    │  START              │
                                    │    │                │
                                    │    ▼                │
                                    │  regexGate ─(hit)─► fallback ──► END
                                    │    │ (safe)         │
                                    │    ▼                │
                                    │  loadHistory        │
                                    │    │                │
                                    │    ▼                │
                                    │  buildMessages      │
                                    │    │                │
                                    │    ▼                │
                                    │  guardedModel ─(attack)─► fallback ──► END
                                    │    │ (safe)         │
                                    │    ▼                │
                                    │  postproc ──► saveHistory ──► END
                                    └─────────────────────┘
                                               ▲
                                               │
                                       Redis (history)
```

### 节点

| 节点 | 职责 |
| --- | --- |
| `regexGate`    | 同步正则黑名单检测，零 IO、零 LLM |
| `loadHistory`  | 从 Redis 读取最近 N 条历史 |
| `buildMessages`| 组装 `system + history + <user_input>...</user_input>` |
| `guardedModel` | 主模型与裁判模型并行；裁判判攻击则立即 cancel 主链 |
| `postproc`     | 清洗回复：折叠空行、按 rune 截断、保证非空 |
| `saveHistory`  | 把本轮 user + assistant 写回 Redis |
| `fallback`     | 从配置池中随机挑一条降级回复 |

> **降级路径不经过 `saveHistory`**——攻击消息不入历史。

### 共享状态 `*flow.State`

节点间用同一个 `*flow.State` 传递，字段随流程逐步填充：

```go
type State struct {
    In       *Input              // 入参快照（只读）
    History  []*schema.Message   // loadHistory 填充
    Messages []*schema.Message   // buildMessages 填充
    Reply    *schema.Message     // guardedModel / fallback 填充
    Verdict  Verdict             // regexGate / guardedModel 填充
}

type Verdict struct {
    Kind   VerdictKind // Safe | Regex | Judge
    Detail string      // 日志用
}
```

Branch 只看 `st.Verdict.Blocked()`，路由决策被类型锁死。

### 并发边界

"主模型 + 裁判并行，任一判攻击立刻断开主链"是 `guardedModel` 节点的内部实现（`errgroup` + `context.WithCancel`）。对 Graph 而言，它仍是一个 `Messages → Reply` 的普通节点，并发细节不污染编排。

---

## 目录

```
llm-bot/
├── cmd/bot/main.go                 进程入口
├── configs/
│   ├── config.yaml                 总配置
│   └── prompts/default.yaml        人设 + 护栏
├── internal/
│   ├── config/                     YAML 加载 + 校验（敏感字段支持 env 覆盖）
│   ├── domain/                     平台无关消息模型
│   ├── adapter/
│   │   ├── adapter.go              Adapter 接口
│   │   └── onebot/                 OneBot v11 反向 WS 实现
│   ├── store/                      Redis 历史存储
│   ├── agent/
│   │   ├── agent.go                Graph 装配
│   │   ├── model.go                OpenAI 兼容 ChatModel
│   │   ├── prompt.go               YAML → Persona
│   │   ├── flow/                   Input / State / Verdict 值类型
│   │   ├── guard/                  regex + judge + guardedModel
│   │   └── nodes/                  loadHistory / buildMessages / postproc / saveHistory / fallback
│   └── bot/                        主循环：Adapter ↔ Agent
├── Makefile
└── README.md
```

---

## 快速开始

### 环境

- Go 1.25+
- Redis 7+
- OpenAI 兼容的 LLM（OpenAI / DeepSeek / Moonshot / OneAPI / Qwen Compat 等）
- NapCatQQ（或其他 OneBot v11 实现）

### 配置

编辑 `configs/config.yaml`：

```yaml
llm:
  base_url: "https://api.deepseek.com/v1"
  api_key:  "sk-xxx"          # 生产建议用环境变量 LLMBOT_LLM_API_KEY 覆盖
  model:    "deepseek-chat"

judge:
  base_url: "https://api.deepseek.com/v1"
  api_key:  "sk-xxx"
  model:    "deepseek-chat"   # 建议选便宜的小模型

redis:
  addr: "127.0.0.1:6379"
```

人设在 `configs/prompts/default.yaml` 中调整，修改后需要重启进程。

> `<user_input>` 标签与 judge 系统提示词是**硬编码**在代码里的（`internal/agent/prompt.go`、`internal/agent/guard/judge.go`）。它们是防御契约的组成部分，不开放给 YAML。

### 运行

```bash
make build
make run
```

默认监听 `:8080`，WS 路径 `/onebot/v11`。

### NapCat 反向 WS 配置

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

> 仅支持数组形态的 `message` 字段（NapCat / 现代 go-cqhttp 默认行为）。旧 CQ 码字符串形态会被当纯文本处理，群聊 `@bot` 触发能力退化为仅靠前缀。

---

## 扩展

| 场景 | 改动点 |
| --- | --- |
| 接入微信 / Telegram | 在 `internal/adapter/` 新增子包实现 `adapter.Adapter`，`main.go` 多起一个实例 |
| 换大模型 SDK | `internal/agent/model.go` 加 switch 分支，必要时拆 `providers/` 子包 |
| 加 Agent 节点 | `internal/agent/nodes/` 新增文件，在 `agent.Build` 的节点表与边表各加一行 |
| 加 RAG 检索 | 在 `loadHistory` 与 `buildMessages` 之间插一个 retriever 节点，把召回文档注入 `state.History` 或 `state.Messages` |
| 工具调用 (ReAct) | 用 `AddToolsNode` 替换 `guardedModel`，model 节点与 tools 节点连成循环边 |
| 新一级防御 | 在 `internal/agent/guard/` 加节点，插在 `regexGate` 前后；必要时扩展 `VerdictKind` |

---

## 设计原则

1. **Graph 是编排的唯一真相来源**。控制流级决策以节点 / 边呈现，不藏在节点内的 if-else。
2. **节点只做一件事**。换实现只改一个文件。
3. **防御契约硬编码**：`<user_input>` 标签、judge 系统提示词、fallback 的 Verdict 规约都不走 YAML，YAML 只放"人设可改内容"。
4. **状态用值类型锁死**：`Verdict` 是枚举 + 值对象，零值即 Safe，不存在 nil / 联动字段。
5. **失败即崩溃**：配置错误启动期 `os.Exit(1)`；裁判失败则 fail-open，由其他防线兜底。

---

## License

见 [LICENSE](./LICENSE)。
