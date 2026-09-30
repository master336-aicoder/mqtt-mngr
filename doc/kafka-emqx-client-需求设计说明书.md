# Kafka / EMQX(MQTT) 双协议消息客户端 —— 需求设计说明书

| 项目 | 内容 |
|------|------|
| 项目名称 | mqtt-mngr（Kafka & EMQX 统一消息管理客户端） |
| 文档版本 | V1.2 |
| 编写日期 | 2026-09-30 |
| 文档状态 | 初稿（待评审） |

**修订记录**

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| V1.0 | 2026-09-30 | 初稿 |
| V1.1 | 2026-09-30 | 新增 FR-6 消息模板管理；原 FR-6 管理面顺延为 FR-7；同步更新数据模型、REST API 与附录示例 |
| V1.2 | 2026-09-30 | 管理功能完备性审查：新增 FR-2.4 Kafka 管理功能清单、FR-2.5 EMQX 管理功能清单、FR-2.6 协议消息级特性补遗、FR-2.7 范围边界；修正 FR-2.2 中"MQTT Topic 不可枚举"表述 |

---

## 1. 引言

### 1.1 编写目的

本文档定义一套同时支持 **Kafka** 与 **EMQX（MQTT 3.1.1 / 5.0）** 两种消息协议的客户端系统的功能需求与非功能需求，作为后续概要设计、详细设计、开发与测试的依据。

### 1.2 项目背景

在物联网与后端数据链路场景中，设备侧普遍使用 MQTT（以 EMQX 为代表），服务端数据管道普遍使用 Kafka。业务系统需要同时接入两类消息中间件，若分别针对两套 SDK 开发，将导致：

- 接入代码重复、配置管理分散；
- Topic/连接生命周期管理不统一；
- 消息解析逻辑无法复用；
- 运维监控口径不一致。

本项目旨在构建一个**统一抽象的双协议消息客户端**，屏蔽 Kafka 与 MQTT 的协议差异，提供一致的服务器管理、Topic 管理、发布/订阅与消息解析能力。

### 1.3 术语与缩写

| 术语 | 说明 |
|------|------|
| Broker | 消息中间件服务端（Kafka 集群 / EMQX 节点） |
| Server 配置 | 客户端连接 Broker 的一组连接参数（地址、认证、TLS 等） |
| Topic | Kafka Topic 或 MQTT Topic（支持通配符） |
| Producer / Consumer | Kafka 生产者 / 消费者 |
| Publisher / Subscriber | MQTT 发布端 / 订阅端 |
| Consumer Group | Kafka 消费组；MQTT 侧对应共享订阅组（`$share/group/topic`） |
| Handler | 业务侧注册的消息回调处理器 |
| DLQ | Dead Letter Queue，死信队列 |
| 消息模板（Message Template） | 预定义的消息字段结构（字段名称、类型、注释等），用于消息的自动归类与结构化解析 |

### 1.4 参考协议与规范

- Apache Kafka Protocol（建议兼容 2.3+ 客户端协议，集群 3.x）
- MQTT 3.1.1（OASis Standard）、MQTT 5.0（OASIS Standard）
- EMQX 5.x 官方文档（认证、共享订阅、系统主题）

---

## 2. 总体设计

### 2.1 系统范围

本系统是一个**可嵌入的客户端库（SDK）+ 管理面**，不包含 Broker 本体的部署运维（提供对接说明，不负责安装 Kafka/EMQX 集群）。

交付形态（三种，按需选其一为主、其余兼容）：

1. **SDK/库**：以包形式供业务进程引入（首选，语言建议 Java/Go，见 §8）；
2. **独立守护进程模式**：将 SDK 包装为常驻服务，暴露 HTTP REST + WebSocket 接口，供多进程共享连接；
3. **CLI 工具**：运维/调试用的命令行（配置管理、收发消息、Topic 浏览）。

### 2.2 总体架构

```
┌────────────────────────────────────────────────────────────┐
│                        业务应用层                            │
│   (注册 Handler / 调用 publish / 管理 server 与 topic)        │
└──────────────┬─────────────────────────────┬───────────────┘
               │ 统一 API（消息客户端接口）      │ 管理 API（CRUD/REST）
┌──────────────▼─────────────────────────────▼───────────────┐
│                      Core：统一消息网关层                     │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌───────────┐ │
│  │ Server 管理 │ │ Topic 管理  │ │ Pub/Sub 引擎│ │ 消息解析   │ │
│  │ Registry   │ │ Manager    │ │ Router     │ │ Codec     │ │
│  └────────────┘ └────────────┘ └────────────┘ └───────────┘ │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  连接池 / 会话管理 / 自动重连 / 心跳 / 限流 / 指标采集     │   │
│  └──────────────────────────────────────────────────────┘   │
└───────────┬───────────────────────────────┬─────────────────┘
            │ Adapter: Kafka                 │ Adapter: MQTT(EMQX)
   ┌────────▼─────────┐            ┌─────────▼──────────┐
   │ Producer/Consumer │            │ Publisher/Subscriber│
   │ Admin Client      │            │ Paho/EMQX 兼容客户端  │
   └────────┬─────────┘            └─────────┬──────────┘
            ▼                                ▼
      Kafka 集群                        EMQX 集群
```

分层要点：

- **Core 层**定义与协议无关的领域模型：`ServerConfig`、`TopicConfig`、`MessageContext`、`Subscription`、`PublishRequest`；
- **Adapter 层**实现协议映射：统一接口 → Kafka/MQTT 原生 API；
- **Codec 层**负责消息序列化/反序列化与协议无关的消息封装（当前仅 JSON，架构上可扩展）。

### 2.3 统一抽象模型（核心设计决策）

Kafka 与 MQTT 概念映射如下，对外统一使用该模型：

| 统一概念 | Kafka 对应 | MQTT/EMQX 对应 |
|----------|-----------|----------------|
| Server | 集群 bootstrap servers | Broker host:port |
| Topic | Topic | Topic（含 `+`/`#` 通配） |
| 订阅 | Consumer Group + assign/subscribe | 订阅 + QoS + 共享订阅组 |
| 发布 | Producer.send | Publish + QoS + retain |
| Key/Partition | record key / partition | 不适用（置空，MQTT 无此概念） |
| Header | record headers | MQTT5 User Properties（3.1.1 降级为 payload 封装） |
| 偏移量语义 | offset | MQTT 无 offset，由 retain/持久会话/共享订阅补偿 |

> 差异处理原则：**取并集建模、按协议能力降级**。MQTT 不支持的能力（如 offset 回溯）在接口中以 `supported=false` 明确暴露，而非静默忽略。

---

## 3. 功能需求

### FR-1 Server 管理（连接/Broker 管理）

#### FR-1.1 Server 配置 CRUD

- **FR-1.1.1** 支持注册/查询/更新/删除一个或多个 Server 配置，配置内容：
  - 通用：`serverId`（全局唯一）、名称、协议类型（KAFKA | MQTT）、启用状态、标签；
  - Kafka：`bootstrapServers[]`、clientId、SASL 机制（PLAIN/SCRAM-SHA-256/SCRAM-SHA-512）、用户名/密码、安全协议（PLAINTEXT/SSL/SASL_SSL）、acks、幂等生产者开关、消费侧参数（自动提交、session.timeout.ms 等可扩展键值）、连接/读写超时；
  - MQTT：`host:port[]`（支持多节点）、协议版本（3.1.1 / 5.0）、clientId 模板（支持 `${hostname}-${pid}-${seq}` 等占位符）、cleanSession/cleanStart、keepAlive(s)、username/password、mTLS 证书引用、requestResponse/responseTopic（MQTT5）、遗嘱消息（LWT）配置、EMQX 特有：共享订阅前缀（默认 `$share`）、管理 API 凭据 `mgmtApi{baseUrl, apiKey, apiSecretRef}`（用于 FR-2.5 的 EMQX 侧管理操作，可选）。
- **FR-1.1.2** 敏感字段（密码、密钥）必须加密存储（见 NFR-4），API 返回时脱敏为 `***`。
- **FR-1.1.3** 支持配置的导入/导出（JSON/YAML 文件），导出时可选择是否携带凭据（默认脱敏）。
- **FR-1.1.4** 更新运行中 Server 的关键连接参数时，需支持"优雅重建连接"（先建新连接、迁移流量、关闭旧连接），不丢订阅。

验收：可通过 API/CLI 完成上述 CRUD；错误配置（如端口非法）返回明确校验错误码。

#### FR-1.2 连接生命周期管理

- **FR-1.2.1** 支持连接模式：
  - `LAZY`：首次使用（订阅/发布）时建连（默认）；
  - `EAGER`：注册配置后立即建连并保持；
  - `MANUAL`：仅显式 `connect()` 后建连。
- **FR-1.2.2** 支持显式 `connect / disconnect / close`；`disconnect` 需等待在途消息发送完成（graceful drain，可配超时）。
- **FR-1.2.3** **自动重连**：断线后按指数退避重连（初始 1s，倍数 2，上限 30s，±20% 抖动，无限次或达上限后告警），重连期间：
  - 发布请求进入本地缓冲（见 FR-3.4）或快速失败（策略可配：`BUFFER | FAIL_FAST`）；
  - MQTT 重连后自动恢复订阅（含 QoS 级别恢复）；Kafka 消费者重入消费组并自动触发 rebalance。
- **FR-1.2.4** **健康检查**：每个 Server 维护健康状态机 `INIT → CONNECTING → CONNECTED → DEGRADED → DOWN`；提供主动探测（Kafka：metadata 请求；MQTT：PINGREQ / 系统主题 `$SYS/broker/uptime`）与被动判定（连续 N 次 IO 失败标记 DOWN）。
- **FR-1.2.5** Kafka 多节点：支持从任一可达 broker 刷新集群元数据；MQTT 多节点：支持按列表顺序/随机 failover（后续版本支持 EMQX 集群返回的备选地址）。
- **FR-1.2.6** 心跳与保活参数可配；MQTT keepAlive 超时必须小于 EMQX 侧配置的 keepalive backoff，文档中给出建议值。

#### FR-1.3 Server 状态查询

- **FR-1.3.1** 查询单/全部 Server 的运行时状态：连接状态、已连接节点地址、版本信息（Kafka broker 版本、EMQX 版本）、上线时长、重连次数、错误计数。
- **FR-1.3.2** 查询 Server 元数据：Kafka 的 broker 列表/Topic 列表/分区数；EMQX 的 connected clients 数（经由 `$SYS` 主题或 EMQX Dashboard API，作为可选增强）。

### FR-2 Topic 管理

#### FR-2.1 Topic 元数据管理（映射管理）

- **FR-2.1.1** 支持在本系统内维护 **Topic 映射配置**（不强制要求 Broker 侧存在）：`topicId`、所属 serverId、Topic 名/模式、方向（PUB | SUB | BOTH）、默认消息格式（JSON）、默认序列化选项、默认 QoS（MQTT）、默认分区键策略（Kafka）、默认消息模板 `templateId`（见 FR-6.3.1）、所属业务域/标签、负责人、备注。
- **FR-2.1.2** Topic 配置支持通配模式登记（MQTT：`sensor/+/temp`、`device/#`；Kafka 不支持通配，登记为精确名）。订阅时可引用模式 ID，实现"按模式批量订阅"。
- **FR-2.1.3** 支持 Topic 配置的分组与检索（按 server、协议、标签、名称模糊查询、分页）。
- **FR-2.1.4** Topic 删除采用软删除，保留审计；删除前检查是否仍被活跃订阅/发布引用并告警。

#### FR-2.2 Broker 侧 Topic 操作（能力矩阵）

| 操作 | Kafka | MQTT/EMQX | 说明 |
|------|-------|-----------|------|
| 创建 Topic | ✅ 支持（Admin API：分区数、副本因子、retention 等配置） | ➖ 无显式创建（首条消息自动生效）；可选对接 EMQX 主题 ACL API | 对 MQTT 返回 `UNSUPPORTED` 或仅登记本地映射 |
| 删除 Topic | ✅ 支持（仅无人消费时） | ➖ 同左（EMQX 无 Topic 删除概念） | 删除前必须二次确认，防止误删 |
| 列出 Topic | ✅ `listTopics` | ✅ 经 EMQX REST API `/topics`（只读，P2）；无 API 凭据时展示本地登记 | 原"Broker 侧不可枚举"表述修正 |
| 修改配置 | ✅ Topic 级 config（retention.ms 等） | ➖ | — |
| 分区信息 | ✅ 分区列表、leader、各分区 LEO/log-end-offset、消费组 lag | ➖ | lag 是 Kafka 监控核心指标，必须实现 |

- **FR-2.2.1** Kafka Topic 的创建/删除由开关 `allowAdminOps` 控制（默认 false），防止业务误操作生产集群。
- **FR-2.2.2** 提供"Topic 浏览"能力：Kafka 实时列出来自 broker；MQTT 列出本地登记 + 通配。

#### FR-2.3 消费组管理（Kafka）与共享订阅（MQTT）

- **FR-2.3.1** Kafka：支持查询消费组列表、成员、offset 提交情况、**消费延迟（lag）**；支持重置 offset（按时间戳/最新/最早，仅 ADMIN 模式）。
- **FR-2.3.2** MQTT：共享订阅以统一语法登记 —— 用户仅需填写业务 topic 与 group 名，Adapter 自动生成 `$share/{group}/{topic}`；非共享订阅则每个订阅者独立收到全量消息。
- **FR-2.3.3** 同一 `subscriptionId` 在同一进程内幂等；重复注册返回已存在错误而非重复订阅。

#### FR-2.4 Kafka 集群管理功能清单（对照 AdminClient 完备性审查）

优先级定义：**P0** = M1 必须；**P1** = M2 交付；**P2** = Backlog（按需排期）。所有写操作统一受 `allowAdminOps` 门控 + 二次确认 + 审计（FR-7.3）。

| # | 功能 | 底层 API | 优先级 | 备注 |
|---|------|----------|--------|------|
| K1 | 集群概览：broker 列表、controller、clusterId、KRaft/ZK 模式 | `describeCluster` | P0 | 补充 FR-1.3 状态查询 |
| K2 | Topic 列表/详情（分区、副本、leader、配置、内部 Topic 标识 `__*`） | `listTopics` / `describeTopics` | P0 | 已有（FR-2.2） |
| K3 | Topic 创建/删除 | `create/deleteTopics` | P1 | 已有（FR-2.2） |
| K4 | 分区扩容 | `createPartitions` | P1 | 只增不减，UI 明示不可逆 |
| K5 | Topic 级配置查询/变更（retention.ms、cleanup.policy 等，逐项增量） | `describe/incrementalAlterConfigs(TOPIC)` | P1 | 已有（FR-2.2 修改配置），细化为逐项 ops 语义 |
| K6 | Broker 动态配置查询/变更 | `describe/incrementalAlterConfigs(BROKER)` | P2 | 影响全集群，危险级门控 |
| K7 | Broker 日志级别动态调整 | `describe/alterConfigs(BROKER_LOGGER)` | P2 | 排障用 |
| K8 | 消费组：列表/详情（状态、成员、分区分配、协议）/删除空组 | `list/describe/deleteConsumerGroups` | P1 | 详情与删除为本项新增 |
| K9 | Offset 管理：committed 查询、earliest/latest/按时间点位查询、重置、删除组 offset | `listOffsets` / `allCommittedOffsets` / 重置工具 / `deleteOffsets`(KIP-802，Kafka 3.8+) | P1 | 重置已有（FR-2.3.1），其余新增 |
| K10 | 日志截断（删至指定 offset，`delete-records` 语义） | `deleteRecords` | P2 | 危险级 |
| K11 | ACL 管理：查询/新增/删除（principal×host×op×resource） | `describe/create/deleteAcls` | P1 | 安全治理配套 |
| K12 | 分区重分配：进行中任务查询；计划提交（仅只读查询为 P1，发起迁移 P2 且建议交由 Cruise Control） | `list/alterPartitionReassignments` | P1/P2 | — |
| K13 | Leader 稳选/失控选举 | `electLeaders(ElectionType)` | P2 | 危险级 |
| K14 | 副本日志目录：查询/变更 | `describe/alterReplicaLogDirs` | P2 | — |
| K15 | 事务管理：进行中事务列表/详情/中止悬挂事务 | `list/describe/abortTransactions` | P2 | Kafka 2.8+ 事务协调器 |
| K16 | Client Quota 管理：查询/设置（produce/consume/request 速率） | `describe/alterClientQuotas` | P2 | — |
| K17 | 特性与元数据版本（KRaft `feature` 升级状态） | `describeFeatures/listFeatures` | P2 | 只读展示 |
| K18 | 消费延迟 lag 全景（组×分区矩阵、热点排序） | 组合 K9 计算 | P0 | 监控核心，UI 看板 |

#### FR-2.5 MQTT / EMQX 管理功能清单（对照 EMQX 5.x REST API 完备性审查）

前提：EMQX 侧管理操作通过 **Dashboard REST API**（`/api/v5/*`，API Key/Secret 认证）实现，凭据在 Server 配置中扩展 `mgmtApi{baseUrl,apiKey,apiSecretRef}`；未配置时相关功能返回 `FEATURE_DISABLED`。同样受 `allowAdminOps` 门控。

| # | 功能 | EMQX API/途径 | 优先级 | 备注 |
|---|------|---------------|--------|------|
| E1 | 节点与集群状态（节点列表、版本、运行时长、队列） | `/nodes`、`$SYS/broker/uptime` | P1 | 补充 FR-1.3 |
| E2 | Broker 指标：当前/历史连接数、订阅数、消息速率 | `/broker`、`/metrics`、`$SYS/#` | P1 | 看板数据源 |
| E3 | 在线客户端：列表/检索（clientId、IP、协议版本、keepalive、连接时间）/单客户端详情（含其订阅） | `/clients`、`/clients/{id}` | P1 | 排障核心 |
| E4 | 强制断开客户端（踢下线） | `DELETE /clients/{node:id}` | P1 | 危险级，二次确认 |
| E5 | 订阅关系：全量/按客户端/按节点列表 | `/subscriptions` | P1 | 与本地订阅登记对账视图 |
| E6 | Topic/路由列表（只读） | `/topics`、`/routes` | P2 | 修正"不可枚举"旧表述 |
| E7 | **Retained 消息：查询/按 topic 删除（清空）** | `/retained`、`DELETE /retained/{topic}` | P1 | retained 生命周期管理，缺失会导致旧值误消费 |
| E8 | 会话：列表/详情/终止会话 | `/sessions` | P2 | — |
| E9 | 告警列表（磁盘告警、节点宕机等） | `/alarm` | P2 | 接入健康状态 DEGRADED 判定 |
| E10 | Listener 状态查询（端口/接受连接数） | `/listeners` | P2 | 只读 |
| E11 | 限速配置查询/修改（global rate limit） | `/rate_limit` | P2 | — |
| E12 | 内置认证用户管理（简单认证模式下增删查） | `/authentication`(built_in_database) | P2 | 复杂认证链（LDAP/JWT）不做，转 Dashboard |
| E13 | 授权 ACL 规则：查询/整体发布规则集 | `/authorization` | P2 | 与 K11 对齐的安全治理 |
| E14 | 黑名单（banned）管理 | `/banned` | P2 | — |

#### FR-2.6 协议消息级特性补遗（收发能力完备性）

| # | 特性 | 协议 | 需求 | 优先级 |
|---|------|------|------|--------|
| M1x | Message-Expiry-Interval：发布可带过期秒数；消费展示剩余有效期 | MQTT5 | 超期消息 EMQX 自动丢弃，本系统透传该属性 | P1 |
| M2x | Subscription Identifier：订阅可注册标识 ID，消息回程携带 | MQTT5 | **可用于携带本系统 `subscriptionId`/模板 ID，增强 FR-6.3 匹配** | P1 |
| M3x | Topic Alias：客户端自动维护别名映射降低上行体积 | MQTT5 | Adapter 内部优化，配置开关 | P2 |
| M4x | Request/Response 模式（response-topic + correlation-data） | MQTT5 | 同步"请求-应答"发布 API `request()`，超时兜底 | P1（V1.1） |
| M5x | Session Takeover / Will Delay Interval | MQTT5 | 建连参数透传 | P2 |
| M6x | 事务性生产者（EOS，read-process-write） | Kafka | `transactional.id` 支持与 handler 内精确提交 | P2 |
| M7x | Kafka 消息时间戳语义（CREATE vs LOG_APPEND） | Kafka | 消费侧时间字段区分展示 | P2 |
| M8x | retained 发布与清除（`retain=true` 空 payload = 删除指令） | MQTT | 发布接口显式支持，与 E7 管理联动 | P1 |

#### FR-2.7 范围边界（明确不纳入本客户端）

以下属于 Broker 平台级运维，仍应使用 Kafka 官方工具 / EMQX Dashboard 完成，本系统不提供（至多只读跳转链接）：

- Kafka：Broker 进程级配置（server.properties）、ZK/KRaft 元数据迁移、镜像/灾备（Mirrormaker）、Schema Registry、Connect/Kafka Streams 平台组件；
- EMQX：规则引擎（Rule Engine）、数据集成/桥接（Connector/Bridge）、Webhook、消息重放（Republish 任务）、Dashboard 用户与 RBAC、集群证书/域名管理、网关协议组件（CoAP/LwM2M 等）、认证数据源结构变更（新建 LDAP/JWT 认证链）；
- 边界原则：**只管理"与消息收发链路直接相关"的对象**（Topic、分区/offset、ACL、客户端会话、retained、限速），平台装配类排除。

### FR-3 消息发布（Publish）

- **FR-3.1** 统一发布接口：`publish(request)` 与 `publishAsync(request) -> Future`。请求字段：
  - 必填：`serverId`、`topic`、`payload`（对象/字符串/字节）；
  - 可选：`key`（Kafka）、`headers/properties`（Map）、`qos`（MQTT 0/1/2）、`retain`（MQTT）、partition（Kafka，高级）、`correlationId`、`templateId`（按模板校验并携带模板标识，见 FR-6）、超时。
- **FR-3.2** 发布前执行：Topic 存在性/权限校验（依据 Topic 映射的方向配置）→ Codec 序列化 → 协议 Adapter 发送。
- **FR-3.3** 结果模型统一：
  - Kafka：返回 `partition / offset`；失败分类可重试/不可重试（如 `TOPIC_AUTHORIZATION_FAILED` 不可重试）；
  - MQTT：QoS0 发出即成功；QoS1/2 以 PUBACK/PUBCOMP 为成功；返回 `packetId`。
- **FR-3.4** **本地缓冲与重试**：连接不可用或发送失败时，按策略处理：
  - 重试：可配次数（默认 3）与退避；
  - 缓冲：内存队列（容量上限 + 溢出策略 `DROP_OLDEST | DROP_NEWEST | BLOCK`），恢复连接后按序重发（至多一次语义，不保证严格顺序的需文档说明）；
  - 全部失败进入本地死信文件/表（可配），供人工或工具重放。
- **FR-3.5** **批量发布**：Kafka 支持批量 send + flush；MQTT 逐条发送但复用连接与限速器。
- **FR-3.6** 发布限速：全局与每 Server 级令牌桶（msg/s 与 bytes/s 双维度），超限快速失败并计数。
- **FR-3.7** 幂等支持：
  - Kafka：开启幂等生产者（enable.idempotence）；
  - MQTT：QoS1 重发语义天然至多一次/至少一次，接口文档明确"本系统对业务承诺**至少一次**投递"。

### FR-4 消息订阅（Subscribe）

- **FR-4.1** 统一订阅接口：`subscribe(subscription, handler)` / `unsubscribe(subscriptionId)`。订阅参数：
  - `serverId`、`topic`（精确或通配模式）、`subscriptionId`（业务唯一）、`group`（Kafka consumer group / MQTT 共享订阅组）、`qos`（MQTT）、并发度、起始位点策略（Kafka：`LATEST | EARLIEST | TIMESTAMP:xxx`）、`autoOffsetCommit`、`templateId`/`templateIds`/`autoMatch`（消息模板关联，见 FR-6.3）。
- **FR-4.2** **Handler 契约**：
  - 入参为统一的 `MessageContext`（见 FR-5.1）；
  - 正常返回 = ACK；抛出异常 = 按重试策略处理；
  - Handler 必须支持并发调用声明（同一 partition 内 Kafka 保序，MQTT 通配多 topic 不保证顺序 —— 文档明示）。
- **FR-4.3** **消费失败处理**：可配 `retryCount`（默认 3，进程内退避重试）→ 超限后转入 **死信投递**（Kafka：`${topic}.DLQ` 并附带失败原因 header；MQTT：发布到 `${topic}.dead`）或丢弃（仅记录日志），策略按订阅粒度配置。
- **FR-4.4** **背压**：内部采用有界队列连接"网络收包线程 → Handler 执行线程池"；队列满时：MQTT 依赖 TCP 流控 + QoS 窗口；Kafka `pause()` 分区拉取。禁止无界队列 OOM 风险。
- **FR-4.5** 支持动态操作：运行期增加/减少订阅、暂停/恢复（`pause/resume`，Kafka 原生、MQTT 通过 unsubscribe+本地缓冲模拟）。
- **FR-4.6** **持久会话**：
  - MQTT：`cleanStart=false` + QoS≥1 时 Broker 保存离线消息（依赖 EMQX 持久会话配置）；
  - Kafka：offset 提交即断点续传；提供手动 ACK 模式（`ack()` 显式调用后提交）。
- **FR-4.7** 单进程内订阅数量上限可配（默认 512），超限拒绝并告警。

### FR-5 消息解析（Codec）

#### FR-5.1 统一消息上下文

`MessageContext` 字段（协议无关）：

```
messageId        // 全局唯一（UUIDv7 或雪花），便于全链路追踪
serverId / topic / direction
timestamp        // 消息时间（broker 或本地）
payloadRaw       // 原始字节
payloadJson      // 解析后的 JSON 结构（ObjectNode/Map）
headers          // Kafka headers / MQTT5 properties 统一映射
protocol         // KAFKA | MQTT
protocolMeta     // partition/offset（Kafka）、qos/retain/dup（MQTT）
subscriptionId   // 消费侧填充
templateId / templateVersion      // 命中的消息模板（见 FR-6）
matchType / matchScore            // EXPLICIT | BOUND | SIMILARITY | NONE 及相似度得分
```

#### FR-5.2 JSON 编解码（V1 唯一必须格式）

- **FR-5.2.1** 发布侧：业务对象（POJO/Map）→ JSON 字节流；支持：字段命名策略（camelCase/snake_case 可配）、null 跳过、日期格式（ISO-8601 默认）、BigDecimal 精度保留、未知字段容错。
- **FR-5.2.3** 消费侧三种模式：
  1. **泛型解析**：`payloadJson` 保留为通用树结构，业务自取；
  2. **类型化解析**：订阅时声明目标类 `Class<T>` / 类型引用，自动反序列化，失败进 DLQ；命中消息模板时走模板感知解析路径（FR-6.4），两者可叠加；
  3. **Schema 校验**（可选增强，V1.1）：登记 JSON Schema，解析前校验；不符合按 `REJECT_TO_DLQ | PASS_THROUGH | LOG_ONLY` 处理。模板与 Schema 定位不同——模板面向"字段语义/注释/归类"，Schema 面向"结构合法性"；模板可导出为 JSON Schema（FR-6.2.3），因此 Schema 校验可复用模板导出物，不重复建设。
- **FR-5.2.4** 支持嵌套 JSON 与顶层数组；非 JSON（如 Bytes、文本）payload 在配置为 JSON 格式时解析失败 → 按 FR-4.3 失败策略处理。
- **FR-5.2.5** 大消息处理：payload 超过阈值（默认 1 MB，可配）时告警并计数；MQTT 受 EMQX `max_packet_size` 限制，超限时发布直接失败并给出明确错误。

#### FR-5.3 可扩展 Codec 框架

- **FR-5.3.1** 定义 SPI 接口 `MessageCodec { name(); supports(topicConfig); encode(...); decode(...); }`；JSON 为内置实现 `json`。
- **FR-5.3.2** Topic 配置中 `format` 字段选择 codec；预留 Avro/Protobuf/Raw/CSV 的注册位置（V1 不实现，仅保证接口可插拔）。
- **FR-5.3.3** headers 中约定 `x-codec: json` 以便消费侧自动识别（发送时自动写入；MQTT5 用 property，MQTT3.1.1 与 Kafka 用 header）。

#### FR-5.4 消息转换器（中间件，V1.1 可选）

- 支持在 Codec 前后挂转换链：字段抽取、脱敏（手机号/身份证掩码）、字段重命名映射（上游系统 A→B 的 JSON 结构映射，基于配置化映射规则而非代码）。

### FR-6 消息模板管理（Message Template）

消息模板是**预定义的消息字段结构描述**（字段名称、类型、注释等）。系统将消息（发布/订阅）与模板关联并自动匹配，实现消息的快速归类、结构化解析与自文档化。

#### FR-6.1 模板定义与 CRUD

- **FR-6.1.1** 模板结构字段：
  - 元信息：`templateId`（全局唯一）、名称、描述、版本号（整数递增）、标签、状态（启用/停用）、负责人；
  - 字段列表 `fields[]`，每项包含：

    | 属性 | 说明 | 必填 |
    |------|------|------|
    | name | 字段名称（支持 JSON Path 点路径表示嵌套，如 `payload.temp`） | ✅ |
    | type | string / integer / number / boolean / object / array / enum | ✅ |
    | required | 是否必填 | ✅（默认 false） |
    | comment | 字段注释（中文，用于文档展示与代码提示） | 建议 |
    | example | 示例值 | 可选 |
    | defaultValue | 缺失时填充的默认值 | 可选 |
    | enumValues | type=enum 时的取值集合 | 条件必填 |
    | nullable | 是否允许 null | 可选（默认 true） |
    | unit/format | 单位或格式（如 ℃、ISO-8601） | 可选 |

  - 嵌套 object / array\<object\> 通过子字段列表递归描述，层级深度默认限制 ≤ 5。
- **FR-6.1.2** 支持模板的创建/查询/更新/删除（软删除）；查询支持按名称/标签模糊检索、分页；模板列表需展示被引用情况（关联的 Topic/订阅数量）。
- **FR-6.1.3** 删除/停用模板前检查引用关系：仍被活跃订阅引用时默认拒绝（`force=true` 解除引用并告警）。

#### FR-6.2 模板维护方式

- **FR-6.2.1 手工定义**：通过 API/CLI/YAML 逐项定义字段。
- **FR-6.2.2 样例反推（推荐主路径）**：提交一条或多条 JSON 样例消息，系统自动**推断生成模板草稿**：
  - 类型推断（含数字/布尔/日期字符串识别，多样例时字段类型取并集判定）；
  - 嵌套结构与数组元素自动展开；
  - 推断字段默认注释为空，提示人工补全；生成结果需人工确认后才正式入库（草稿态）。
- **FR-6.2.3 导入/导出**：支持 JSON Schema 的导入与导出（双向），保证与现有 schema 生态互通；导出文件不含运行时统计信息。

#### FR-6.3 消息与模板的关联及自动匹配

- **FR-6.3.1 静态关联**：Topic 映射配置与订阅请求均可指定 `templateId`（或 `templateIds[]` 候选集合）与 `autoMatch` 开关：

  | 关联点 | 配置 | 语义 |
  |--------|------|------|
  | Topic 配置 | `templateId` | 该 Topic 的默认模板（发布/订阅继承） |
  | 订阅请求 | `templateId` / `templateIds` | 精确指定或限定候选范围 |
  | 发布请求 | `templateId` | 按模板校验并写入 header `x-template: {id}@{ver}` |

- **FR-6.3.2 自动匹配优先级**（消费侧对每条消息执行，短路求值）：
  1. **Header 显式指定**：消息头 `x-template`（Kafka header / MQTT5 property）携带模板 ID → 直接命中；
  2. **订阅/Topic 静态关联**：按 FR-6.3.1 的绑定关系命中；
  3. **结构相似度匹配**：在候选模板集（订阅的 `templateIds`，未指定则为该 server 下全部启用模板）中计算命中分数：
     `score = Σ(字段名命中 × 类型一致加权) / max(消息字段数, 模板字段数)`，
     嵌套字段按 JSON Path 全路径比较；取 score 最高且 ≥ 阈值（默认 0.8，可配）者为匹配结果，平分时取版本号最新者；
  4. 无命中 → 归入 `UNCLASSIFIED`，仅做泛型 JSON 解析并计数告警。
- **FR-6.3.3 匹配结果落地**：`MessageContext` 增加 `templateId / templateVersion / matchType(EXPLICIT|BOUND|SIMILARITY|NONE) / matchScore` 字段；指标按模板维度聚合（每模板的消息量/匹配率/解析失败率），实现**消息快速归类**。
- **FR-6.3.4 匹配缓存**：相同 JSON 结构指纹（字段名+类型 hash）的匹配结果进程内缓存（LRU，默认 1024 条），避免逐条全量相似度计算；模板变更时缓存失效。
- **FR-6.3.5** CLI 提供 `mngr template match <server> <topic> --sample file.json` 预览匹配结果与得分，便于配置调优。

#### FR-6.4 基于模板的解析与校验

- **FR-6.4.1** 命中模板后，JSON Codec 走**模板感知解码路径**：
  - 按字段类型做转换与校验（如 "36.8" → number、epoch → 日期）；
  - 必填缺失 / 类型不符 → 按订阅的失败策略处理（重试→DLQ，复用 FR-4.3）；
  - 有 `defaultValue` 的缺失字段自动填充；
  - 未声明字段按 `unknownFields: KEEP | DROP | REJECT` 策略处理（默认 KEEP）。
- **FR-6.4.2** 类型化还原：`ctx.as(Class<T>)` 时以模板字段↔类属性映射驱动；SDK 侧支持由模板**生成 POJO/struct 骨架代码**（含注释转 doc 注释，CLI `mngr template gen <templateId> --lang java|go`）。
- **FR-6.4.3** 发布侧模板校验：指定 `templateId` 发布时，发送前按模板校验 payload（必填齐全、类型正确），校验失败快速拒绝，避免脏消息流入 Broker。
- **FR-6.4.4** 模板即文档：API/CLI 输出模板的字段表格（名称/类型/必填/注释/示例），供对接方直接查阅消息格式。

#### FR-6.5 模板版本与演进

- **FR-6.5.1** 模板更新产生新版本（`templateId` 不变，`version` 递增），保留历史版本供追溯；消息记录命中时的版本号。
- **FR-6.5.2 兼容性检测**：新增可选字段 = 兼容变更；删除字段、类型变更、必填收紧 = 不兼容变更——订阅默认 `followLatest=true` 时遇不兼容变更须显式确认（或锁定 `version`）。
- **FR-6.5.3** 模板变更事件写入审计日志（FR-7.3），并使相关匹配缓存失效。

验收：
1. 用样例 JSON 反推生成模板并补注释，关联订阅后，同类消息自动归类且类型化还原正确；
2. 结构相近的多模板 Topic 下，相似度匹配得分与命中模板符合预期（CLI 可复现）；
3. 不符合模板的消息进入 DLQ 且携带失败字段明细；
4. 模板不兼容变更被拦截并要求显式确认。

### FR-7 管理与可观测

- **FR-7.1 指标**：按 server/topic/subscription 维度输出 —— 发布/订阅 TPS、字节速率、成功/失败数、重试数、耗时分布（P50/P95/P99）、连接状态、重连次数、Kafka lag、缓冲队列深度、DLQ 计数；另按**消息模板**维度输出消息量、匹配率、解析失败率（见 FR-6.3.3）。暴露 Prometheus `/metrics` 端点（守护进程模式）或对接 Micrometer（SDK 模式）。
- **FR-7.2 日志**：关键生命周期事件（建连、重连、订阅变更、配置变更）结构化日志；消息级日志可配采样率防止刷屏。
- **FR-7.3 审计**：Server/Topic/模板配置的变更记录（who/when/before/after），本地存储或接入外部审计日志。
- **FR-7.4 配置存储**：默认 SQLite/文件（YAML）两种本地持久化；守护进程模式可用 PostgreSQL/MySQL 作为中心存储（V1.1）。
- **FR-7.5 CLI**：`mngr server list|add|rm`、`mngr topic list|add`、`mngr template list|add|infer|gen|match`、`mngr pub <server> <topic> -f file.json [--template id]`、`mngr sub <server> <topic> [--group] [--seconds 10]`、`mngr status`。用于验收与排障。

---

## 4. 非功能需求（NFR）

| 编号 | 需求 | 指标 |
|------|------|------|
| NFR-1 | 性能 | 单实例发布 ≥ 10,000 msg/s（1KB JSON，Kafka）/ ≥ 5,000 msg/s（MQTT QoS1）；消费解析吞吐 ≥ 5,000 msg/s；Codec 单条 P99 < 1ms |
| NFR-2 | 延迟 | MQTT QoS0 本地→broker P99 < 10ms（同机房）；消费回调延迟（不含业务）P99 < 20ms |
| NFR-3 | 可靠性 | 至少一次投递；进程非正常退出重启后，Kafka 未提交 offset 消息可重放、MQTT 持久会话离线消息可接收；本地缓冲消息尽量恢复（落盘可选，默认内存） |
| NFR-4 | 安全 | 凭据 AES-256-GCM 加密存储，主密钥来自环境变量/KMS；支持 TLS 1.2+ 双向认证（SASL_SSL / mTLS）；REST 管理接口需 Token 鉴权；日志脱敏 |
| NFR-5 | 可用性 | 库自身故障不影响业务进程；重连、恢复全自动无人工干预；单 Server 故障隔离，不影响其他 Server 收发 |
| NFR-6 | 资源占用 | SDK 常驻内存 < 256MB（默认配置）；无泄漏，168h 压测 RSS 增长 < 10% |
| NFR-7 | 兼容性 | Kafka 集群 2.8+/3.x；EMQX 4.4+/5.x；MQTT 3.1.1 与 5.0 双版本 |
| NFR-8 | 可扩展 | 新协议（如 Pulsar/RocketMQ）仅需新增 Adapter，不改 Core；新格式仅需新增 Codec |
| NFR-9 | 可测试性 | 单元覆盖率 ≥ 70%；集成测试用 testcontainers（Kafka/EMQX 容器）；提供内置 Mock Broker 供离线单测 |

---

## 5. 接口设计（示意，SDK 优先）

### 5.1 核心 API（伪代码，Java 风格）

```java
// —— Server 管理 ——
serverMgr.register(ServerConfig cfg);            // 校验并保存
serverMgr.update(String serverId, ServerConfig cfg);
serverMgr.remove(String serverId, boolean force);
List<ServerView> serverMgr.list(ServerQuery q);
ServerStatus serverMgr.status(String serverId);  // 健康状态+指标
serverMgr.connect(String serverId);  /  disconnect(String serverId);

// —— Topic 管理 ——
topicMgr.declare(TopicConfig cfg);               // 本地映射登记
topicMgr.bind(String topicId, String serverId, String name);
List TopicView> topicMgr.list(TopicQuery q);
kafkaAdmin.createTopic(ref, partitions, replicas); // allowAdminOps=true 时
kafkaAdmin.deleteTopic(ref);
List<LagInfo> kafkaAdmin.consumerLag(String group);

// —— 消息模板 ——
templateMgr.save(TemplateDef t);                 // 创建/新版本
templateMgr.infer(String templateName, List<String> jsonSamples); // 样例反推草稿
TemplateView templateMgr.get(String templateId); / list(TemplateQuery q);
templateMgr.delete(String templateId, boolean force);
MatchResult templateMgr.matchPreview(String serverId, String topic, String sampleJson);

// —— 发布 ——
PublishResult      pub.publish(PublishRequest req);
CompletableFuture<PublishResult> pub.publishAsync(PublishRequest req);

// —— 订阅 ——
SubscriptionHandle sub.subscribe(SubscribeRequest req, MessageHandler h);
sub.unsubscribe(String subscriptionId);
sub.pause(String subscriptionId); / resume(...);

// —— 解析 ——
<T> T MessageContext.as(Class<T> type);          // 类型化取值
JsonNode MessageContext.json();                  // 泛型取值
```

### 5.2 管理 REST（守护进程模式）

```
POST   /api/v1/servers                注册 Server
GET    /api/v1/servers?protocol=MQTT  列表
PATCH  /api/v1/servers/{id}           更新
DELETE /api/v1/servers/{id}           删除
GET    /api/v1/servers/{id}/status    状态与指标
POST   /api/v1/servers/{id}/_connect  连接/_disconnect
GET    /api/v1/topics                 Topic 映射列表
POST   /api/v1/topics                 登记 Topic
DELETE /api/v1/topics/{id}            软删除
POST   /api/v1/publish                发布（body 即 PublishRequest）
GET    /api/v1/subscriptions          活跃订阅列表
DELETE /api/v1/subscriptions/{id}     取消订阅
GET    /api/v1/templates              模板列表（引用计数、分页）
POST   /api/v1/templates              创建模板
GET    /api/v1/templates/{id}         模板详情（含版本历史）
PATCH  /api/v1/templates/{id}         更新（产生新版本，兼容性检测）
DELETE /api/v1/templates/{id}         软删除
POST   /api/v1/templates/_infer       样例 JSON 反推模板草稿
GET    /api/v1/templates/{id}/export  导出 JSON Schema
POST   /api/v1/templates/_match       匹配预览（sample → 候选模板+得分）
WS     /api/v1/ws/subscribe?topic=..  调试用实时推流
GET    /api/v1/metrics                Prometheus 文本
```

统一响应：`{ code, message, data, requestId }`；错误码分段：`1xxx` 参数校验、`2xxx` 配置/资源冲突、`3xxx` 连接类、`4xxx` 发送类、`5xxx` 解析类。

### 5.3 数据模型（持久化实体）

```
server_config(id, name, protocol, enabled, conn_json, cred_enc, mode, created_at, updated_at)
topic_config (id, server_id, name_pattern, direction, format, qos, key_strategy, template_id, tags, owner, deleted, ...)
subscription(id, server_id, topic_id, group, handler_ref, state, options_json, ...)
msg_template (id, name, version, status, fields_json, source, owner, tags, created_at, updated_at, deleted)
template_bind(id, scope_type /* TOPIC|SUBSCRIPTION */, scope_id, template_id, template_version, auto_match)
dlq_message  (id, server_id, topic, payload, headers, error, created_at)
audit_log    (id, actor, action, object_type, object_id, before_json, after_json, created_at)
```

---

## 6. 关键流程

### 6.1 发布流程

```
业务 publish → 参数/权限校验 → Codec.encode(JSON) → 限速器
  → Adapter(Kafka send / MQTT publish)
      ├─ 成功 → 返回 PublishResult(offset/packetId)
      └─ 失败 → 可重试? → 退避重试(n次) → 缓冲队列/立即失败
                              └─ 最终失败 → DLQ + 指标 + 日志
```

### 6.2 消费流程

```
Broker 推送/拉取 → 有界队列（背压） → 线程池取 worker
  → Codec.decode（JSON→MessageContext）→ Schema校验(可选)
  → Handler.handle(ctx)
      ├─ 正常 → ACK（Kafka 提交 offset / MQTT PUBACK 由库自动）
      └─ 异常 → 重试 → 超限 → 投递 DLQ topic
```

### 6.3 重连流程

```
检测断线 → 状态置 DEGRADED → 指数退避重试建连
  → 成功 → 恢复订阅(MQTT 重发 SUBSCRIBE / Kafka 重入组) → 排空缓冲 → CONNECTED
  → 达上限 → 状态 DOWN → 告警 → 继续低频探测（60s）
```

---

## 7. 边界与异常场景清单（测试关注点）

1. Kafka Topic 不存在且禁自动创建 → 发布报错 `UNKNOWN_TOPIC_OR_PARTITION`，不得无限重试；
2. EMQX 认证失败（bad user/passwd）→ 状态 DOWN 并停止密码重试风暴（退避加倍）；
3. 重连风暴：网络抖动 1s 内断 50 次 → 退避生效、日志限流；
4. MQTT 通配订阅 `a/#` 与精确订阅 `a/b` 同时存在 → 消息按订阅各自投递一次（不去重，文档明示业务方）;
5. JSON payload 顶层为字符串 `"ok"`（非对象）→ 解析失败进 DLQ；
6. Handler 抛未捕获异常/Handler 卡死 → 超时（默认 30s）中断并计失败，不得拖垮整个订阅线程池（每订阅隔离池或公平调度）；
7. offset 提交失败（Kafka 组协调器抖动）→ 至多一次承诺不成立，需重放，验证幂等文档；
8. 缓冲区满 + 持续断连 → 溢出策略生效且指标可见；
9. 删除仍在使用的 Server → 默认拒绝（`force=true` 才级联清理订阅）；
10. 大消息（>1MB）MQTT 发布 → 与 EMQX `max_packet_size` 协同的明确错误信息；
11. 两个模板字段结构高度相似（区分度 < 阈值）→ 相似度匹配平分取新版本，并输出 `matchScore` 供排查；得分低于阈值归入 UNCLASSIFIED 而非误归类；
12. 模板不兼容变更（字段类型改变）且订阅 followLatest → 显式拦截并要求确认，不得静默切换。

---

## 8. 技术选型建议

> **已决议（2026-09-30，V1.1）**：采用 Java / Spring Boot + Vue3 管理台、Netty（经 HiveMQ MQTT Client）、MyBatis + 内置 H2（提供无数据库 file 模式）。详见《kafka-emqx-client-系统技术设计说明书.md》，下表保留作对比记录。

| 组件 | Java 方案（推荐，若团队为 Java 栈） | Go 方案（若倾向轻量部署） |
|------|-----------------------------------|--------------------------|
| Kafka 客户端 | `org.apache.kafka:kafka-clients` 3.7+ | `segmentio/kafka-go` 或 `confluent-kafka-go` |
| MQTT 客户端 | `Eclipse Paho MQTTv5` 1.2+ 或 **HiveMQ MQTT5 Client**（推荐，对 5.0 支持更好） | `eclipse/paho.mqtt.golang`（3.1.1+5.0） |
| JSON | Jackson | `encoding/json` + `sonic`（性能可选） |
| 指标 | Micrometer + Prometheus | prometheus/client_golang |
| 本地存储 | SQLite（JDBC）| modernc.org/sqlite |
| 守护进程 Web | Spring Boot / Vert.x | gin + gorilla/websocket |
| 集成测试 | Testcontainers（kafka、emqx 镜像） | testcontainers-go |

> 决策项：主语言需在评审会上确定，影响 §2.1 交付形态细节；本文其余需求与语言无关。

---

## 9. 里程碑规划（建议）

| 阶段 | 范围 | 说明 |
|------|------|------|
| M1（V1.0） | FR-1、FR-2.1~2.3 + FR-2.4 之 P0（K1/K2/K18）、FR-3、FR-4、FR-5（JSON 泛型/类型化）、FR-6（模板 CRUD、样例反推、静态关联、模板感知解析）、SDK 形态、CLI 基础命令 | 核心闭环：双协议收发自如 + 模板归类解析 |
| M2（V1.1） | 守护进程 + REST/WS、Prometheus、DLQ 重放工具、JSON Schema 校验、配置中心存储、FR-6（相似度自动匹配、版本兼容检测、代码生成 `template gen`）、FR-2.4 之 P1（K3~K5、K8~K9、K11、K12 只读）、FR-2.5 之 P1（E1~E5、E7，需 EMQX API 凭据）、FR-2.6 之 P1（M1x/M2x/M4x/M8x） | 运维与治理 |
| M3（V1.2+） | 消息转换链/脱敏、FR-2.4/FR-2.5 之 P2 项（K6/K7、K10、K12 发起迁移、K13~K17、E6、E8~E14）、FR-2.6 之 P2（M3x/M5x/M6x/M7x）、多协议扩展（Pulsar 等） | 增强 |

## 10. 风险与开放问题

| # | 风险/问题 | 应对 |
|---|-----------|------|
| 1 | Kafka 与 MQTT 语义差异（顺序、at-most/at-least-once）被业务误用 | 接口文档 + `supported=false` 显式降级，评审时逐条确认 |
| 2 | MQTT 3.1.1 无 header，元信息只能塞 payload | 约定封装信封格式或强制 MQTT5（倾向后者，待确认） |
| 3 | EMQX 共享订阅需 4.0+ 且与 cleanStart 组合复杂 | 集成测试矩阵覆盖 |
| 4 | 凭据主密钥管理（环境变量易扩散） | V1 用 env/KMS 约定，文档明确运维职责边界 |
| 5 | 待确认：是否需要跨语言客户端（如 Java server + JS web 调试台）？ | 评审决议 |

## 11. 验收标准（摘要）

1. 同一份业务代码，仅切换 `serverId`（分别指向 Kafka/EMQX）即可完成发布与订阅，不改业务逻辑；
2. 断网 30s 恢复后：订阅自动恢复、缓冲消息按策略发出，全程无人工干预；
3. JSON 消息端到端：发布对象 → 消费端类型化还原，字段一致率 100%；
4. 压测达到 NFR-1/NFR-2 指标；§7 全部异常场景行为符合本文档描述；
5. CLI 完成 `server/topic/pub/sub` 四类操作闭环演示；
6. 消息模板闭环：样例反推 → 关联订阅 → 自动匹配归类（`matchType/matchScore` 可查）→ 类型化解析，§FR-6 验收条目全部通过。

---

## 附录 A：Topic 映射配置示例（YAML）

```yaml
servers:
  - id: kafka-prod
    protocol: KAFKA
    bootstrapServers: ["kafka-1:9092", "kafka-2:9092"]
    security: { protocol: SASL_SSL, saslMechanism: SCRAM-SHA-512 }
    mode: LAZY
  - id: emqx-prod
    protocol: MQTT
    mqttVersion: "5.0"
    hosts: ["emqx-1:1883", "emqx-2:1883"]
    clientIdTemplate: "mngr-${hostname}-${seq}"
    keepAliveSec: 30
    cleanStart: false
    sharedSubscriptionPrefix: "$share"

topics:
  - id: t-device-event
    server: emqx-prod
    pattern: "device/+/event"
    direction: BOTH
    format: json
    qos: 1
    templateId: tpl-device-event      # 默认消息模板（FR-6.3.1）
    autoMatch: true
  - id: t-order-paid
    server: kafka-prod
    name: order-paid
    direction: SUB
    format: json
    group: mngr-default
    templateIds: [tpl-order, tpl-order-refund]  # 候选模板集合，相似度自动归类
```

## 附录 B：消息模板示例（YAML / 反推 JSON）

```yaml
templates:
  - id: tpl-device-event
    name: 设备事件消息
    version: 3
    description: 温湿度设备上报事件
    fields:
      - { name: deviceId,  type: string,  required: true,  comment: 设备编号, example: temp-01 }
      - { name: temp,      type: number,  required: true,  comment: 摄氏温度, unit: "℃", example: 36.8 }
      - { name: humidity,  type: number,  required: false, comment: 相对湿度（%），缺省不填充 }
      - { name: ts,        type: string,  required: true,  comment: 上报时间, format: ISO-8601 }
      - { name: alarm,     type: enum,    required: false, comment: 告警级别, enumValues: [NONE, WARN, CRIT], defaultValue: NONE }
      - { name: battery,   type: object,  required: false, comment: 电池信息,
          fields: [ {name: level, type: integer, comment: 电量百分比},
                    {name: voltage, type: number, comment: 电压V} ] }
```

```bash
# 由样例消息一键反推模板草稿
mngr template infer --name 设备事件消息 --sample ./device-event.json
```

## 附录 C：Publish / Subscribe 请求示例（JSON）

```json
// POST /api/v1/publish
{
  "serverId": "emqx-prod",
  "topic": "device/temp-01/event",
  "payload": { "deviceId": "temp-01", "temp": 36.8, "ts": "2026-09-30T10:00:00+08:00" },
  "qos": 1,
  "retain": false,
  "templateId": "tpl-device-event",
  "headers": { "traceId": "abc-123" }
}

// 订阅（SDK 伪代码）：关联候选模板，自动匹配归类
sub.subscribe(SubscribeRequest.builder()
    .serverId("kafka-prod").topic("order-paid")
    .group("mngr-default").fromLatest()
    .templates("tpl-order", "tpl-order-refund").autoMatch(true)
    .retry(3).dlq(true)
    .build(),
  ctx -> {
    OrderEvent e = ctx.as(OrderEvent.class);   // 模板感知解析（FR-6.4）
    log.info("template={} v{} score={}", ctx.templateId(), ctx.templateVersion(), ctx.matchScore());
  });
```
