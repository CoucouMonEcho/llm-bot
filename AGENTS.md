# AGENTS.md

面向 AI 编码助手的项目开发上下文。只写"改代码时必须知道的事"，运维 / 启动 / 命令 / Redis 调试请直接看 [`README.md`](README.md) 与 [`Makefile`](Makefile)。

---

## 1. 一句话

Go 写的 LLM 聊天机器人骨架：用 [eino](https://github.com/cloudwego/eino) `compose.Graph` 串 9 个节点跑被动回复链路，配套人设 stats / 长期记忆 / 主动消息三条独立侧路。当前仅对接 OneBot v11，新平台只需新增 Adapter 子包。

---

## 2. Graph 与中断语义

**9 节点链路** ── 见 [`internal/agent/agent.go`](internal/agent/agent.go) 顶部 ASCII 图：

```text
START → judgeGate → loadContext → lowStateGate → buildMessages
       → chatModel → postproc → saveHistory → updateMemory → scoreStats → END
```

任何节点返回 [`flow.ErrSkipReply`](internal/agent/flow/errors.go) → Graph 中断 → Bot 静默不回复 → **不入历史 / 不更新 memory / 不打分**。可能触发的位置：

- `judgeGate`：judge 模型 nil / prompt 空 / 调用失败 / 输出非 `safe`（fail-closed）；
- `lowStateGate`：低好感度或低心情下按线性概率（最高 50%）跳过；
- `postproc`：清洗后回复为空。

主动消息侧路完全独立：[`internal/proactive/scheduler.go`](internal/proactive/scheduler.go) 后台轮询 → 选 bot 沉默最久且超阈值的群 → [`internal/proactive/generator.go`](internal/proactive/generator.go) 生成开场白 → 复用 `Adapter.Send` → 写一条 assistant-only 群历史并刷新 `bot_proactive_group_last_spoke`。

---

## 3. 目录与依赖方向

```text
cmd/llm-bot/           进程入口（main + 装配）
internal/
├── app/bot/           Bot 主循环、并发限流、follow-up gate、回复样式决策
├── agent/             Agent 装配（agent.go）+ Persona + Model 构造
│   ├── flow/          叶子包：Input / State / VerdictKind / ErrSkipReply
│   └── nodes/         9 个 Lambda 节点（含 judgeGate）
├── config/            YAML 加载 + 环境变量覆盖 + 校验
├── domain/            平台无关消息模型
├── infra/store/       Redis 客户端 + HistoryRepo + GroupBufferRepo
├── llmtext/           小工具：LoadPromptFile / StripCodeFence
├── memory/            长期记忆 Store + 异步 Update Dispatch
├── platform/adapter/  IM 抽象 + onebot/ 子包
├── proactive/         主动消息：State / Generator / Scheduler / Prompts
└── stats/             好感度 + 心情 Store + 异步打分 Dispatch
```

**依赖方向（必须遵守）**：

- `flow` 是叶子包，**不能** import 任何项目内其他包；
- `nodes` 通过 `BuildFunc` 闭包接收 `Persona.BuildMessages`，避免 `nodes → agent` 循环（见 [`internal/agent/nodes/build_messages.go`](internal/agent/nodes/build_messages.go) 包 doc）；
- `agent` 装配只 import `nodes`（**没有** `guard` 子包，judge 在 `nodes/judge_gate.go` 里）；
- `proactive` 通过最小 `Sender` / `HistoryWriter` 接口接受 adapter / store，不反向耦合；
- `flow.Input.Platform` / `ConvType` 用 `string` 而非 `domain.*` 类型——刻意保持，避免 `flow → domain` 反向依赖。

---

## 4. 核心契约

### 4.1 软降级（最高优先级）

stats / memory / proactive / GroupBuffer 任何读写失败 → 只 `log.Warn`，**不阻断主对话链**。新增任何"装饰性能力"都必须满足这条。
见 [`internal/stats/stats.go`](internal/stats/stats.go) 包 doc 设计约束 1~3、[`internal/memory/memory.go`](internal/memory/memory.go)、[`internal/proactive/scheduler.go`](internal/proactive/scheduler.go)、[`internal/infra/store/group_buffer.go`](internal/infra/store/group_buffer.go)。

### 4.2 异步副作用走独立 ctx

`stats.Dispatch` / `memory.Dispatch` 内部都用 `context.Background()` 派生独立超时，**不要**传入 Bot 的请求 ctx——回复发送完 ctx 立刻 cancel，传进去会让异步任务一起被取消。

### 4.3 Persona 不可变

[`internal/agent/persona.go`](internal/agent/persona.go) `*Persona` 启动时固化、运行期只读、多 goroutine 共享。`BuildMessages` 用 `strings.Builder` 在栈上拼新 system，**不要写回 `p.SystemPrompt`**。

### 4.4 维度键约定

| 资源 | 维度 | Redis key 形态 |
|---|---|---|
| 对话历史 | sessionID | `bot_hist_<sessionID>` |
| 群聊普通文本缓存 | sessionID | `bot_groupbuf_group_<groupID>` |
| 好感度 | platform + userID | ZSET `bot_stats_affinity_rank`，member `<platform>_<userID>` |
| 心情 / 上次结算时间 | 全局 | HASH `bot_stats_global` |
| 长期记忆 | platform + userID | `bot_memory_<platform>_<userID>` |
| 主动消息开关 | 全局 | `bot_proactive_enabled` |
| Bot 群内最后发言时间 | sessionID | HASH `bot_proactive_group_last_spoke` |

群聊回复同时双写一份 `bot_hist_private_<userID>`（"群里搭话也算认识你"），双写在 [`internal/agent/nodes/save_history.go`](internal/agent/nodes/save_history.go) 与 [`internal/agent/nodes/load_context.go`](internal/agent/nodes/load_context.go) 内闭环，平台无关层不感知。

### 4.5 触发策略与 GroupBuffer 写入时机

群聊普通消息**默认不进 Graph**。只有 `InboundMessage.ExplicitTrigger=true`（@bot / 命令前缀）或落在 follow-up 窗口（`trigger.group_followup_sec`）内，才会被 [`internal/app/bot/bot.go`](internal/app/bot/bot.go) `shouldInvokeGraph` 放行。

被 follow-up gate 拒绝的群聊普通消息 → 由 **Bot 层** `cacheGroupBackground` 写入 `bot_groupbuf_*`（onebot adapter **不**直接写 GroupBuffer）。@bot 时 `loadContext` 节点 `LRANGE 0 -1` 读全量并渲染成 system prompt 的"群里最近的对话片段"块。

### 4.6 主动消息决策面只有三件事

**群冷却 + 时间窗 + Redis 开关**——不要再加白名单 / 日限额 / 会话冷却 / 用户活跃时间窗（已被刻意删除，见 [`internal/proactive/scheduler.go`](internal/proactive/scheduler.go) 与 [`internal/proactive/state.go`](internal/proactive/state.go) 包 doc 的"刻意删除"段）。运行期开关在 Redis `bot_proactive_enabled`，YAML 只配置部署期参数。

### 4.7 stats 边界 = prompt 契约

[`internal/stats/stats.go`](internal/stats/stats.go) 第 73~99 行的 `affMin/affMax/moodMin/moodMax/affDeltaMax/moodDeltaMax` 是 [`configs/prompts/stats/score.md`](configs/prompts/stats/score.md) 里声明范围的对应面，改一边必须同步另一边。**不要**把这些常量外部化为配置。

memory 字数上限 `memory.max_chars` 同理与 [`configs/prompts/memory/update.md`](configs/prompts/memory/update.md) 配套。

---

## 5. 红线清单

- 不要在主链路加任何**同步**的 LLM / Redis 调用；新增装饰性能力一律 `Dispatch` 异步。
- 不要绕过 `store.HistoryRepo` / `store.GroupBufferRepo` 直拼 Redis key——所有 key 都属于对应包，统一前缀 `bot_<模块>_`。
- 不要在 `internal/agent/flow` 里加业务逻辑或反向依赖（叶子包约束）。
- 不要把 `Persona.SystemPrompt` 改成可写；不要在 `BuildMessages` 内 mutate persona。
- 不要把 `flow.Input.Platform` / `ConvType` 改成 `domain.*` 类型——是刻意保持的字符串。
- 不要给 `proactive` 加白名单 / 日限额 / 会话冷却 / 用户活跃时间窗（已被刻意删除）。
- 不要在 main 里把 stats / memory / GroupBuffer 关闭分支用"哨兵对象"代替——节点内部按 `nil` 安全跳过。
- 不要把 stats 打分 / memory 更新挂到 Adapter 发送成功上——已在 `scoreStats` / `updateMemory` 节点（"回复已生成"时机）触发。
- 群聊 `bot_groupbuf_*` 写入方是 **Bot 层** `cacheGroupBackground`，不是 onebot adapter——不要在 adapter 里直接写。

---

## 6. 测试现状与建议

仓库当前**无 `*_test.go` 文件**。新增测试时优先覆盖**纯函数 / table-driven** 场景（不需要真实 Redis / LLM）：

| 函数 | 文件 |
|---|---|
| `pickOldestIdle` / `inTimeWindow` / `parseHHMM` / `nextDelay` | [`internal/proactive/scheduler.go`](internal/proactive/scheduler.go) |
| `regressMood` / `clamp` / `affinityPromptLabel` / `moodPromptLabel` | [`internal/stats/stats.go`](internal/stats/stats.go) |
| `lowStateGate` / `linearSkipProbability` | [`internal/agent/nodes/low_state_gate.go`](internal/agent/nodes/low_state_gate.go) |
| `decideReplyTarget` / `targetReplyLatency` | [`internal/app/bot/bot.go`](internal/app/bot/bot.go) |
| `renderGroupBackground` | [`internal/agent/nodes/load_context.go`](internal/agent/nodes/load_context.go) |
| `cleanGeneratedText` / `containsForbiddenFragment` | [`internal/proactive/generator.go`](internal/proactive/generator.go) |
| `parseGeneratorMarkdown` / `normalized` | [`internal/proactive/prompts.go`](internal/proactive/prompts.go) |

写完后顺手把 [`Makefile`](Makefile) 第 8 行注释（"当前项目还没有测试用例"）一起更新。

---

## 7. 小角落约定

- 文本长度统一按 **rune** 而非 byte：`postproc.maxReplyRunes=800`、`decideReplyTarget` 12/35、`targetReplyLatency` 12/35、`renderGroupBackground.groupBackgroundMaxChars=1200`。
- 历史 List：`LPUSH` 新消息在 index 0；`Load` 时反转为"旧→新"喂 LLM。
- TTL 策略：history 30d 滑动、group_buffer 默认 10m 滑动、stats / memory / proactive 永不过期。
- 日志：用 `slog`；info 用于"被拦截路径 + 事件型动作"，debug 用于正常路径详情。
- 时间字段在 YAML 用秒（易读），代码内统一 `time.Duration`，由 [`internal/config/config.go`](internal/config/config.go) 的方法（`Interval()` / `JitterMax()` / `BotSilenceThreshold()` / `TTL()` / `Timeout()`）转换。
- Redis key 分隔符全用 `_`（不是 `:`）。
