# llm-bot

用 Go 写的 LLM 聊天机器人骨架。把 "接 IM 协议、跑 LLM 回复链路、维持人设状态、可控主动发消息" 四件事拆成各司其职的模块。当前对接 OneBot v11（NapCatQQ 等），新平台只需新增一个 Adapter 子包。

## 功能

- **被动回复**：用 [eino](https://github.com/cloudwego/eino) `compose.Graph` 把 9 个节点串成一条回复链路，拦截、低状态不回复和收尾副作用都收在节点边界里。
- **前置注入裁判**：独立 LLM 裁判先判断输入，只有明确输出 `safe` 才读取上下文并调用主模型；非安全输入会静默不回复。
- **人设参数 stats**：好感度按 "平台+用户" 维度累计、心情全局共享并按沉默时长自然回归；每轮回复后异步打分写回 Redis。
- **长期记忆**：按 "平台+用户" 保存压缩事实摘要；回复后异步合并更新，与短期对话历史分离。
- **群聊触发策略**：普通群文本也会进入 `Bot`；`Bot` 只在 @ / 前缀等显式触发，或短时间连续对话窗口内，把消息送进 Agent Graph。
- **群聊短期上下文**：未触发 Graph 的普通群文本被 `Bot` 写入一条 Redis List（`LPUSH+LTRIM+EXPIRE`，单群最多 20 条 / 10 分钟）；下次 @bot 时由 `loadContext` 直读全量并渲染成 system prompt 里的"刚才群里在聊什么"块，不进个人 history、不污染 stats / memory。
- **主动发消息**（默认关闭）：扫一遍"bot 群内最后发言时间"HASH，挑出 bot 沉默超过 1h 的群发条短消息；发送成功后写入群历史，时间窗与 Redis 总开关收口在调度器。
- **OpenAI 兼容**：主模型与 judge 都走 OpenAI Chat Completions 协议；DeepSeek、Qwen 兼容模式、OneAPI、Ollama 都能直连。

软降级是通用约定：stats / memory / proactive 失败只打日志，永远不阻断对话主链。

## 架构

```mermaid
flowchart LR
    NapCat[NapCatQQ] -- "反向 WS" --> Adapter
    Adapter -->|InboundMessage| Bot
    Bot -->|显式触发 / 连续对话| Graph[Agent Graph]
    Graph -->|OutboundMessage| Adapter
    Bot -.->|群发送成功| Redis[(Redis)]
    Scheduler --> Generator --> Adapter
    Redis --> Scheduler
```

被动回复 Graph：

```mermaid
flowchart TD
    START([START]) --> judgeGate
    judgeGate -- safe --> loadContext
    judgeGate -- "non-safe" --> skipReply
    loadContext --> lowStateGate
    lowStateGate -- reply --> buildMessages
    lowStateGate -- skip --> skipReply
    buildMessages --> chatModel
    chatModel --> postproc
    postproc --> saveHistory --> updateMemory --> scoreStats
    scoreStats --> END([END])
    skipReply[skipReply] --> END
```

| 节点 | 作用 |
|------|------|
| `judgeGate` | 前置 LLM 裁判；只有明确输出 `safe` 才放行，非安全输入静默不回复 |
| `loadContext` | 拉本轮 prompt 需要的 stats / memory / history 上下文 |
| `lowStateGate` | 根据低好感度 / 低心情做概率性不回复，最高 50%；命中时直接不发消息，不走兜底话术 |
| `buildMessages` | 组装 system + history + 当前 user 消息 |
| `chatModel` | 调用主模型 Generate，写入原始回复 |
| `postproc` / `saveHistory` / `updateMemory` / `scoreStats` | 清洗回复 → 落历史 → 异步更新长期记忆 → 异步打分 |
| `skipReply` | 静默结束：不发消息、不入 history、不触发回复后副作用 |

静默中断路径由节点返回 `ErrSkipReply` 触发；Graph 中断后不会进入 `saveHistory` / `updateMemory` / `scoreStats`。

主动消息侧路完全独立：`Bot` 在群消息发送成功后记录 bot 本次开口时间；`Scheduler` 后台轮询 → 扫这份 HASH 选 bot 最久未开口且超过 idle 阈值的群 → `Generator` 拉同群最近几条历史作语气参考 → 生成开场白 → 同一个 `Adapter.Send` 发出 → 发完写入一条 assistant-only 群历史并回写本次开口时间。

### 目录

```text
internal/
├── app/            应用层编排：Bot 主循环、并发控制
├── agent/          eino Graph 装配
│   ├── flow/       Input / State / VerdictKind
│   └── nodes/      judgeGate + 其余 Graph 节点
├── config/         YAML + 环境变量
├── domain/         平台无关消息模型
├── infra/          Redis 客户端 + 对话历史 / 群聊缓存 Repo
├── llmtext/        共享小工具：加载 prompt、剥 markdown 代码块
├── memory/         长期记忆 Store + 异步更新
├── platform/       IM 抽象 + OneBot v11 反向 WS 实现
├── proactive/      State / Generator / Scheduler
└── stats/          好感度 / 心情 Store + 异步打分
```

## Redis 存储速查

所有 key 形如 `bot_<模块>_<...>`。下表是当前**全集**，写下来供排查时直接 `redis-cli`。

### 对话历史 — `internal/infra/store`

| Key | 类型 | 内容 | TTL |
|-----|------|------|-----|
| `bot_hist_<sessionID>` | List | 一条 JSON / 元素，`LPUSH` 新消息在头部 | 30d 滑动 |
| `bot_groupbuf_<sessionID>` | List | 群聊短期上下文，每元素一条 JSON `{uid,un,c,ts}` | `group_buffer.ttl_sec` 滑动（默认 10m） |

`<sessionID>` 形如 `private_123456` 或 `group_789012`。**被动群聊回复会同时再写一份 `bot_hist_private_<userID>`**——这是 "群里搭话也算认识你" 的私聊维度记忆，`loadContext` 时会去重合并回主线。主动群消息只写一条 assistant-only 群历史，不写个人历史。

`bot_groupbuf_*` 仅在群聊会话出现，且只由 `Bot` 在"消息被 follow-up gate 决定不进 Graph"的分支写入，避免与 `bot_hist_*` 重复。`Agent` 侧 `loadContext` 节点对群聊会话直接 `LRANGE 0 -1` 全量读取并渲染——窗口大小由写入侧 `LTRIM 0 max-1` 与 `EXPIRE` 双重管控。

```bash
LRANGE bot_hist_private_123456 0 4
# {"role":"assistant","content":"达咩哟","ts":"2026-04-29T13:51:00+08:00"}
# {"role":"user","content":"扮演 chatgpt","name":"123456","ts":"2026-04-29T13:50:59+08:00"}
```

### Stats — `internal/stats`

| Key | 类型 | 字段 / Member | 含义 |
|-----|------|---------------|------|
| `bot_stats_affinity_rank` | ZSET | member=`<platform>_<userID>`，score=affinity | 全局好感度排行，[-100, 100] |
| `bot_stats_global` | Hash | `mood`（int [-50, 50]）、`last_chat_at`（Unix 秒） | 全局心情 + 上次结算时间 |

均**永不过期**——人设是长期累积。

```bash
ZSCORE bot_stats_affinity_rank onebot_123456     # → "12"
HMGET  bot_stats_global mood last_chat_at        # → "8" "1714378260"
ZREVRANGE bot_stats_affinity_rank 0 9 WITHSCORES # 好感度 Top10
```

### 长期记忆 — `internal/memory`

| Key | 类型 | 内容 | TTL |
|-----|------|------|-----|
| `bot_memory_<platform>_<userID>` | String | 中文事实摘要，≤ `memory.max_chars` | 永不过期 |

```bash
GET bot_memory_onebot_123456
# → "喜欢猫；最近在准备考试；不喜欢被人催。"
```

### 主动消息 — `internal/proactive`

| Key | 类型 | 内容 | TTL |
|-----|------|------|-----|
| `bot_proactive_enabled` | String | `"true"` / `"false"`，运行期总开关 | 永不过期 |
| `bot_proactive_group_last_spoke` | Hash | field=`group_<id>`，value=bot 最近一次在该群发言的 Unix 秒 | 永不过期 |

`bot_proactive_group_last_spoke` 是写入面也是读取面：`Bot` 被动群回复发送成功后刷新，`Scheduler` 主动群消息发送成功后也刷新；调度器 HGETALL 一次拿到全部候选群与上次开口时间。冷启动时该 HASH 不存在，调度器自然不发消息；bot 在某个群里成功说过话后，该群从那一刻起进入候选。

运行期管理无内置命令，直接 `redis-cli`：

```bash
SET     bot_proactive_enabled true                  # 打开
DEL     bot_proactive_enabled                       # 关闭
HGETALL bot_proactive_group_last_spoke              # 看候选群与 bot 上次开口时间
HDEL    bot_proactive_group_last_spoke group_789    # 把某个群从候选中清除
```

## 运行

环境：Go 1.25+ / Redis / OpenAI 兼容 LLM API / OneBot v11 实现（NapCatQQ 等）。

```bash
make build && make run
# 或
go run ./cmd/llm-bot -config configs/config.yaml
```

敏感配置走环境变量覆盖：

```bash
export LLMBOT_LLM_API_KEY="sk-..."
export LLMBOT_JUDGE_API_KEY="sk-..."
export LLMBOT_REDIS_PASSWORD="..."
export LLMBOT_SERVER_ACCESS_TOKEN="..."
```

人设与主动消息 prompt 在 `configs/prompts/persona/`，护栏、打分契约、记忆更新契约在 `configs/prompts/` 下按职责分目录保存。改完重启进程才生效。

## 注意

- 调度器按**单进程**设计，没有分布式锁。多实例部署时应只让一个实例跑 `Scheduler`，或在外部补互斥。
- 主动消息只有一个开关：Redis `bot_proactive_enabled`。默认 unset 等价于关闭；要启用 `redis-cli SET bot_proactive_enabled true`。
- 主动消息只面向群聊，群冷却阈值在 `proactive.idle_threshold_sec`（默认 1h）；私聊不在覆盖范围内。

## License

MIT，见 [LICENSE](./LICENSE)。
