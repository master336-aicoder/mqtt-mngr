# Kafka / EMQX 双协议消息客户端 —— 系统技术设计说明书

| 项目 | 内容 |
|------|------|
| 项目名称 | mqtt-mngr（Kafka & EMQX 统一消息管理客户端） |
| 文档版本 | V1.1 |
| 编写日期 | 2026-09-30 |
| 关联文档 | [需求设计说明书](./kafka-emqx-client-需求设计说明书.md)（V1.2） |
| 技术体系 | Java 17 / Spring Boot 3 / Netty / MyBatis / H2（内置）/ Vue 3 |

**修订记录**

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| V1.0 | 2026-09-30 | 初稿：确定 Spring Boot + Vue3 体系、H2/无数据库双存储模式、Maven 多模块工程结构与核心接口设计 |
| V1.1 | 2026-09-30 | 管理功能完备性审查同步：新增 §8.3 集群管理实现（KafkaOps / EmqxMgmtClient）、`ClusterOpsGateway` SPI、管理台"集群管理"页；对齐需求书 FR-2.4~FR-2.6 |

---

## 1. 概述

### 1.1 文档目的

在需求说明书（FR-1 ~ FR-7、NFR）基础上，给出**可落地的技术架构与详细设计**：技术选型、工程结构、分层与模块职责、核心接口、存储设计（含无数据库降级模式）、并发模型、前后端设计与部署方案，作为开发实施依据。

### 1.2 交付形态决策（响应需求 §2.1）

本项目以**守护进程模式为主体交付**：

- 单个 Spring Boot 可执行 JAR（`mngr-server`），内置 Web 管理台（Vue3 静态资源打包进 JAR）；
- REST + WebSocket 管理/数据接口；
- 同时保留 **SDK 依赖方式**：业务工程直接引入 `mngr-core` + adapter 模块以库方式内嵌使用（不走 HTTP）；
- CLI（`mngr-cli`）复用同一 Core。

### 1.3 存储模式决策（本次新增约束）

采用**双模式存储**，通过配置切换，同一套 `Repository` 接口两种实现：

| 模式 | `mngr.store.mode` | 实现 | 适用场景 |
|------|-------------------|------|----------|
| 内置数据库（默认） | `h2` | H2 file 模式 + MyBatis | 正式部署：配置持久化、审计、DLQ、运行状态查询 |
| 无数据库 | `file` | YAML 配置文件 + 内存运行态（DLQ/审计写 JSONL 文件） | 轻量/嵌入式部署、容器只读文件系统、单文件交付 |

> 设计原则：**Core 层不感知存储技术**，仅依赖 `ConfigRepository` / `RuntimeRepository` 接口；无库模式下 H2、MyBatis 相关 Bean 通过 `@ConditionalOnProperty` 完全不加载。

---

## 2. 总体技术架构

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────┐
│  前端管理台（Vue 3 SPA，构建产物内置于 JAR 的 static/）         │
│  Server管理 │ Topic管理 │ 模板管理 │ 收发调试台 │ 监控看板      │
└───────────────▲─────────────────────────▲───────────────────┘
             REST/JSON (HTTP)        WebSocket(实时消息流/状态推送)
┌───────────────┴─────────────────────────┴───────────────────┐
│                mngr-server（Spring Boot 3 / MVC）             │
│  Controller ─ DTO校验 ─ 统一响应/异常 ─ Token鉴权 ─ CORS       │
│  WebSocket Handler（STOMP 简化协议：订阅推流、状态事件）        │
├─────────────────────────────────────────────────────────────┤
│                     mngr-core（协议无关核心）                   │
│  ServerManager │ TopicManager │ TemplateManager │ Pub/Sub引擎  │
│  CodecRegistry │ ConnectionRegistry(状态机) │ Metrics │ Store SPI│
├──────────────┬──────────────┬───────────────────────────────┤
│ mngr-store   │ mngr-codec-  │ mngr-adapter-kafka              │
│ (h2/file双实 │ json(Jackson)│ (kafka-clients)                 │
│  现, MyBatis)│              ├─────────────────────────────────┤
├──────────────┴──────────────┤ mngr-adapter-mqtt               │
│  持久化                      │ (HiveMQ MQTT Client → Netty IO)│
└─────────────────────────────┴─────────────────────────────────┘
            ▼                    ▼
        Kafka 集群            EMQX 集群
```

### 2.2 分层职责

| 层 | 模块 | 职责 | 禁止事项 |
|----|------|------|----------|
| 接入层 | mngr-server | REST/WS、鉴权、参数校验、错误码转换 | 不得包含业务逻辑；不得直接访问存储 |
| 核心层 | mngr-core | 领域模型、生命周期编排、状态机、路由、指标 | 不得依赖 Spring Web / MQTT / Kafka SDK |
| 协议层 | mngr-adapter-* | 实现 `ProtocolAdapter` SPI，协议↔统一模型映射 | 不得反向依赖核心层以外的模块 |
| 解析层 | mngr-codec-json | JSON 编解码、模板感知解析、Schema 转换 | — |
| 存储层 | mngr-store | Repository SPI 双实现（H2+MyBatis / 文件） | 不得包含业务规则 |
| 前端 | mqtt-mngr-ui | Vue3 SPA | 只消费 REST/WS，无业务态持久化（Pinia 内存态） |

### 2.3 关键设计决策（ADR 摘要）

| # | 决策 | 理由 | 备选与否决 |
|---|------|------|-----------|
| D1 | MQTT 客户端选 **HiveMQ MQTT Client 1.3.x**（底层 Netty 4.1） | 原生支持 MQTT 5 + 3.1.1，Reactive API，Netty 复用团队技术栈；TLS/重连内建 | Paho（5.0 支持一般、内部线程模型不透明）；megalion（EMQX 维护但社区活跃度低） |
| D2 | Kafka 用官方 `kafka-clients`，不引入 Spring Kafka | 需要精细控制 Consumer 线程/offset ACK/pause-resume（FR-4），Spring Kafka 抽象过厚 | Spring Kafka 否决 |
| D3 | Web 层用 **Spring MVC（Servlet）**，非 WebFlux | 管理台 QPS 低；MVC + MyBatis 同步栈更简单可控；消息推流用 WebSocket 单独处理 | WebFlux 否决（全链路响应式成本高） |
| D4 | 内置库选 **H2 file 模式**，MyBatis-Plus 作 ORM | 零安装、单文件、MySQL 兼容模式；MyBatis 符合技术栈要求 | SQLite 否决（驱动部署复杂于纯 Java 场景略差，H2 与 Spring 生态更顺） |
| D5 | 前后端一体打包（frontend-maven-plugin） | 交付 `java -jar mngr-server.jar` 单文件即用 | 分离部署保留为可选运维模式（nginx 托管 dist） |
| D6 | 无库模式以"YAML 单一事实来源 + JSONL 追加"实现 | 满足"无数据库设计"约束且可 Git 管理配置 | 纯内存否决（重启丢配置） |
| D7 | 线程模型：平台线程池 + 结构化队列；Java 17（暂不上 21 虚拟线程） | 消费者数量可控、背压语义依赖显式队列；降低初期复杂度 | 记录演进点：升 Java 21 后 Handler 池可换虚拟线程 |

---

## 3. 技术选型与版本基线

| 类别 | 组件 | 版本 | 用途 |
|------|------|------|------|
| 语言/运行时 | Java | 17 LTS | 基线 |
| 框架 | Spring Boot | 3.3.x | Web/容器/配置 |
| | spring-boot-starter-web | 3.3.x | REST（Tomcat 内置） |
| | spring-boot-starter-websocket | 3.3.x | WS 推流 |
| | spring-boot-starter-validation | 3.3.x | Bean Validation |
| 消息协议 | kafka-clients | 3.7.x | Kafka Producer/Consumer/Admin |
| | hivemq-mqtt-client | 1.3.x | MQTT 3.1.1/5.0 客户端 |
| | netty-* | 4.1.x（随 hivemq 传递，显式锁定版本） | MQTT IO、TLS(ALPN) |
| 解析 | Jackson (databind/core) | 2.17.x | JSON、REST payload |
| | json-schema-validator (networknt) | 1.4.x | 模板→Schema 校验（V1.1） |
| 存储 | com.h2database:h2 | 2.2.x | 内置库 |
| | mybatis-plus-spring-boot3-starter | 3.5.x | ORM/映射 |
| | HikariCP | 5.x（Boot 默认） | 连接池（H2 单文件，池上限 4） |
| | snakeyaml | 2.x | file 模式配置读写 |
| 观测 | micrometer-registry-prometheus | 1.13.x | 指标 |
| 安全 | 自实现 Token（`X-Mngr-Token`）+ AES-256-GCM (JDK) | — | 管理面鉴权/凭据加密 |
| 工具 | lombok | 1.18.x | 样板代码 |
| | picocli | 4.7.x | mngr-cli |
| 测试 | junit5 / mockito / testcontainers(kafka, emqx) | 最新稳定 | 单测+集成 |
| 前端 | Vue | 3.4+ (Composition API + TS) | SPA |
| | Vite | 5.x | 构建 |
| | Element Plus | 2.7.x | 组件库 |
| | Pinia / Vue Router / axios / ECharts | 最新稳定 | 状态/路由/HTTP/图表 |

> 依赖统一由父 POM `dependencyManagement` 锁定；Netty 版本经 `mvn dependency:tree` 校验无冲突（kafka-clients 自带 shaded netty 不冲突）。

---

## 4. 工程结构

### 4.1 Maven 多模块

```
mqtt-mngr/
├── pom.xml                      # 父 POM（BOM、插件、Java17）
├── mngr-core/                   # 领域模型与引擎（无 Spring Web 依赖）
├── mngr-codec-json/             # JSON Codec + 模板解析
├── mngr-adapter-kafka/
├── mngr-adapter-mqtt/
├── mngr-store/                  # Repository SPI + h2/file 双实现
├── mngr-server/                 # Spring Boot 应用（REST/WS/静态资源）
├── mngr-cli/                    # picocli 命令行（package 阶段 native 可选）
├── mqtt-mngr-ui/                # Vue3 前端（npm 工程，frontend-maven-plugin 联动）
└── doc/
```

### 4.2 mngr-core 包结构（核心）

```
io.mngr.core
├── model/          # ServerConfig TopicConfig SubscriptionDef PublishRequest
│                   # MessageContext PublishResult TemplateDef FieldDef
├── enums/          # Protocol ConnMode ServerState MatchType Direction ...
├── spi/            # ProtocolAdapter MessageCodec ConfigRepository
│                   # RuntimeRepository TemplateMatcher
├── server/         # ServerManager ConnectionRegistry HealthProbe Backoff
├── topic/          # TopicManager WildcardMatcher
├── pub/            # PublisherEngine RateLimiter BufferQueue DlqWriter
├── sub/            # SubscriberEngine DispatchPool BackpressureQueue
├── template/       # TemplateService Matcher(相似度) Cache(结构指纹)
│                   # Inferer(样例反推) VersionGuard
├── metrics/        # MngrMetrics（Micrometer 封装）
└── exception/      # MngrException + 错误码枚举 ErrorCode
```

### 4.3 `ProtocolAdapter` SPI（协议扩展点，NFR-8）

```java
public interface ProtocolAdapter {
    Protocol protocol();                                  // KAFKA | MQTT

    void connect(ServerConfig cfg, ConnCallback cb);      // 异步建连，回调状态机
    void disconnect(String serverId, Duration drain);
    ServerRuntime status(String serverId);

    CompletableFuture<PublishResult> publish(PublishRequest req, byte[] payload);

    String startSubscription(SubscriptionDef def, Consumer<RawIncoming> sink); // 返回 handle
    void stopSubscription(String serverId, String handle);
    void pause(String subId);  void resume(String subId);

    Optional<TopicAdmin> topicAdmin();                    // Kafka 提供；MQTT 空
    boolean supports(Capability cap);                     // OFFSET_REWIND/ORDERED/...
}
```

`RawIncoming`（协议原始入件）由 Core 的 `DispatchPipeline` 统一走 Codec → Template → Handler（§7.2），Adapter 不承担解析职责。

---

## 5. 存储设计

### 5.1 H2 模式：表结构 DDL（`schema.sql`，Flyway 不做，随 Boot `sql.init` 幂等建表）

```sql
CREATE TABLE IF NOT EXISTS server_config (
  id            VARCHAR(64) PRIMARY KEY,
  name          VARCHAR(128) NOT NULL,
  protocol      VARCHAR(16)  NOT NULL,          -- KAFKA|MQTT
  enabled       BOOLEAN      NOT NULL DEFAULT TRUE,
  conn_mode     VARCHAR(16)  NOT NULL DEFAULT 'LAZY',
  conn_json     CLOB         NOT NULL,          -- 非敏感连接参数(序列化)
  cred_enc      VARBINARY(1024),                -- AES-GCM 密文(口令/证书引用)
  tags          VARCHAR(512),
  created_at    TIMESTAMP    NOT NULL,
  updated_at    TIMESTAMP    NOT NULL
);
CREATE TABLE IF NOT EXISTS topic_config (
  id            VARCHAR(64) PRIMARY KEY,
  server_id     VARCHAR(64)  NOT NULL,
  name_pattern  VARCHAR(512) NOT NULL,
  direction     VARCHAR(8)   NOT NULL,          -- PUB|SUB|BOTH
  format        VARCHAR(16)  NOT NULL DEFAULT 'json',
  qos           TINYINT,
  key_strategy  VARCHAR(32),
  template_id   VARCHAR(64),
  auto_match    BOOLEAN      NOT NULL DEFAULT TRUE,
  tags          VARCHAR(512),
  owner         VARCHAR(64),
  remark        VARCHAR(512),
  deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
  updated_at    TIMESTAMP    NOT NULL
);
CREATE TABLE IF NOT EXISTS msg_template (
  id            VARCHAR(64),
  version       INT,
  name          VARCHAR(128) NOT NULL,
  description   VARCHAR(512),
  status        VARCHAR(16)  NOT NULL DEFAULT 'ENABLED',  -- ENABLED|DISABLED|DRAFT
  fields_json   CLOB         NOT NULL,          -- FieldDef[] 树
  source        VARCHAR(16)  NOT NULL,          -- MANUAL|INFERRED|SCHEMA_IMPORT
  owner         VARCHAR(64), tags VARCHAR(512),
  deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMP, updated_at TIMESTAMP,
  PRIMARY KEY (id, version)
);
CREATE TABLE IF NOT EXISTS subscription (
  id            VARCHAR(64) PRIMARY KEY,
  server_id     VARCHAR(64) NOT NULL,
  topic_id      VARCHAR(64),
  topic         VARCHAR(512) NOT NULL,
  grp           VARCHAR(128),
  state         VARCHAR(16)  NOT NULL,          -- RUNNING|PAUSED|STOPPED
  options_json  CLOB,                            -- 含 templateIds/autoMatch/qos/retry/dlq
  created_at    TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS dlq_message (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  server_id VARCHAR(64), topic VARCHAR(512), subscription_id VARCHAR(64),
  template_id VARCHAR(64), template_version INT,
  headers_json CLOB, payload BLOB(4MB),
  err_code INT, err_msg VARCHAR(1024),
  created_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dlq_time ON dlq_message(created_at);
CREATE TABLE IF NOT EXISTS audit_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  actor VARCHAR(64), action VARCHAR(32), obj_type VARCHAR(16), obj_id VARCHAR(64),
  before_json CLOB, after_json CLOB, created_at TIMESTAMP NOT NULL
);
```

MyBatis-Plus：仅 `server_config/topic_config/msg_template/subscription` 用 Mapper + `IService`；`dlq_message/audit_log` 用 `JdbcTemplate` 批量插入（写多读少，避免映射开销）。H2 URL：

```
jdbc:h2:file:${mngr.data-dir}/db/mngr;MODE=MySQL;AUTO_SERVER=FALSE;DB_CLOSE_ON_EXIT=FALSE
```

凭据加解密在 Repository 层完成（`CredCipher`：AES-256-GCM，密钥 = SHA-256(`MNGR_MASTER_KEY` env)，IV 随机前置存入 `cred_enc`）。

### 5.2 file 模式（无数据库设计）

```
${mngr.data-dir}/
├── config/
│   ├── servers.yaml      # ConfigRepository.saveServer → 全量重写(文件锁)
│   ├── topics.yaml
│   ├── templates/
│   │   └── {templateId}.yaml     # 当前版本；history/ 存版本快照
│   └── subscriptions.yaml        # 持久订阅定义(重启自动恢复)
├── data/
│   ├── dlq.jsonl         # 追加写；轮转 64MB×5
│   └── audit.jsonl
└── state/                # 运行态快照(仅诊断用，可丢弃)
```

要点：

- **写策略**：YAML 整文件重写（配置量小，百条级 <10ms），`FileLock` + 临时文件 `ATOMIC_MOVE` 保证不损坏；
- **凭据**：file 模式同样 AES-GCM 加密后以 `credEnc: base64` 存 YAML，主密钥仍来自环境变量；
- **查询能力降级**：无库模式不支持 DLQ 条件分页查询（仅尾部扫描 + 全量导出），`RuntimeRepository.capabilities()` 显式声明，REST 返回 `503 FEATURE_DISABLED(file)` 而非静默；
- 两模式行为差异表随 README 交付。

### 5.3 Repository SPI

```java
public interface ConfigRepository {           // mngr-core.spi
  void saveServer(ServerConfig c);  Optional<ServerConfig> findServer(String id);
  List<ServerConfig> listServers(ServerQuery q);  void deleteServer(String id);
  // topic / template(含 version 历史) / subscription 同构方法省略
}
public interface RuntimeRepository {
  void appendDlq(DlqRecord r);  Page<DlqRecord> queryDlq(DlqQuery q);
  void audit(AuditEvent e);     Set<RuntimeCapability> capabilities();
}
```

---

## 6. mngr-server（Spring Boot）设计

### 6.1 启动装配

```java
@SpringBootApplication
@Import({CoreConfig.class, StoreConfig.class, KafkaAdapterConfig.class, MqttAdapterConfig.class})
public class MngrServerApplication { ... }
```

- `CoreConfig`：装配 Manager 们（构造注入 SPI 实现列表 → 按 `Protocol` 建 `Map<Protocol, ProtocolAdapter>`）；
- `StoreConfig`：`@ConditionalOnProperty(name="mngr.store.mode", havingValue="h2")` → MyBatis/DataSource Bean；`havingValue="file"` → `FileConfigRepository/FileRuntimeRepository`；
- 启动顺序：恢复配置 → 注册 Adapter → `EAGER` 模式 Server 建连 → 恢复持久订阅 → 暴露端口；
- 优雅停机（`server.shutdown=graceful`）：停 WS 推送 → 断订阅(提交 offset) → drain 发布缓冲(默认 10s 超时) → 关闭 Adapter → 关 H2。

### 6.2 REST 层规范

| 项 | 约定 |
|----|------|
| 基础路径 | `/api/v1`，资源命名见需求 §5.2 |
| 响应包装 | `ApiResponse<T>{code,message,data,requestId}`；成功 `code=0` |
| 错误码 | 沿用需求分段：`1xxx` 校验、`2xxx` 配置冲突、`3xxx` 连接、`4xxx` 发送、`5xxx` 解析、`6xxx` 模板（新增：6001 模板不存在、6002 不兼容变更未确认、6003 模板被引用禁止删除、6004 匹配阈值未达） |
| 全局异常 | `@RestControllerAdvice`：`MngrException→对应码`；`MethodArgumentNotValidException→1001`；其他→9999 + ERROR 日志(traceId) |
| 分页 | `?page=0&size=20&sort=updatedAt,desc`，返回 `PageResp{items,total}` |
| 鉴权 | Filter 校验 `X-Mngr-Token`（配置 `mngr.auth.token` 非空才启用）；`/api/v1/public/**`、静态资源、`/actuator/health` 放行 |
| CORS | 开发态允许 `localhost:5173`；生产同源（JAR 内置静态页） |

控制器清单：`ServerController TopicController TemplateController PublishController SubscriptionController DlqController StatusController KafkaMgmtController EmqxMgmtController`（与需求 FR 对应关系见 §12）。

### 6.3 WebSocket 设计

- 端点 `/ws`（原生 `TextWebSocketHandler`，不引 STOMP——消息量小、协议自定义更轻）；
- 客户端→服务端控制帧（JSON）：

```json
{ "action": "subscribe",  "wsId": 1, "serverId": "emqx-prod", "topic": "device/+/event", "tail": true }
{ "action": "publish_try", "wsId": 2, "request": { ...同 REST publish } }
{ "action": "watch_status", "wsId": 3, "serverId": "kafka-prod" }
```

- 服务端→客户端：`msg_frame`（一条消息 = messageId/topic/templateId/matchScore/payloadJson/headers，解析失败附 `parseError`）、`result`、`event`（Server 状态迁移、订阅变更，由 Core `EventBus`→Guava-free 自研 `ListenerRegistry` 广播）、`ack`；
- 推流上限：每连接背压检查，发送队列 > 2000 帧则丢帧并计数（FR-4.4 精神，仅调试通道）；调试 WS **不影响** SDK 订阅。

---

## 7. 核心引擎设计（mngr-core）

### 7.1 连接与状态机

`ConnectionRegistry` 持有每 Server 的 `ConnSlot{state, client handle, retryCount, lastError}`：

```
INIT ──connect()──▶ CONNECTING ──ok──▶ CONNECTED
   ▲                   │fail               │io-fail/ping-timeout
   │                   ▼                   ▼
   └─────────────  DOWN  ◀──────  DEGRADED（重连中，退避调度）
                     │ 低频探测(60s)
                     └────────────▶ CONNECTING
```

- 退避器 `BackoffPolicy`：1s ×2 封顶 30s ±20% 抖动（NFR）；MQTT 复用 HiveMQ client 内建重连（关闭其自动重连改用自研统一策略，保证两端行为一致与指标可观测）；
- 重连成功钩子：MQTT 由 Adapter 重发 `SUBSCRIBE`（订阅清单从 Core `SubscriptionRegistry` 取）；Kafka 消费者重新 `subscribe()` 触发 rebalance；发布缓冲 `BufferQueue` 排空按序重发。

### 7.2 消费分发管线

```
Adapter sink（Netty IO 线程 / Kafka consumer poll 线程）
   → BackpressureQueue（有界，默认 8192，满则 Kafka pause / MQTT 靠 TCP 流控）
   → DispatchPool（每 subscription 独立虚拟…否——每 subscription 固定 worker 数 1..n 可配，
                   平台线程，命名 mngr-dispatch-{subId}-{i}）
   → Codec.decode（JSON→JsonNode；模板感知路径 §7.3）
   → TemplateResolver.match(ctx)（§7.3）
   → HandlerInvoker（30s 超时 guard：Future.get(timeout)，超时计失败不占死 worker）
   → ACK / 重试 / DLQ 投递
```

线程池参数：`corePool = min(并发度, CPU×2)`，队列即背压队列，拒绝策略 `CallerRuns` 禁止（防阻塞 IO 线程），实为阻塞取任务模式。

### 7.3 模板匹配与模板感知解析（FR-6 落地）

**匹配短路链**（每消息一次，指纹缓存兜底 O(1)）：

1. `headers["x-template"]` → 解析 `id@version`，命中登记模板则 `EXPLICIT`；
2. 订阅绑定 `templateId(s)`：单绑定直接命中 `BOUND`；候选集进入 3；
3. 相似度：`StructureFingerprint`（字段全路径 + 类型桶 hash）查 LRU(1024)；未命中则计算
   `score = Σ hit(name)*w_type / max(|F_msg|,|F_tpl|)`（`w_type`：类型一致 1.0、number↔integer 0.8、兼容容器 0.5）；`score ≥ threshold(0.8)` 取最大、平分取 `version` 大者 → `SIMILARITY`；
4. 否则 `NONE` → `UNCLASSIFIED` 指标 +1。

**解析**：命中模板后 `TemplateCodec` 将 `JsonNode` 按 `FieldDef[]` 驱动转换（类型矫正、默认值填充、必填校验、`unknownFields` 策略），产出 `TypedPayload{Map<String,Object> values, JsonNode raw}`；`ctx.as(Class)` 用 Jackson `convertValue`（模板字段名↔属性映射表缓存于 `TemplateRuntime`）。模板变更后：版本号+1、指纹缓存按 `templateId` 失效、`VersionGuard` 比对 diff（字段删除/类型变更/必填收紧 = 破坏性，订阅非锁定版本时抛 6002 要求 `force` 确认）。

**样例反推**（`TemplateInferer`）：多样例合并——同路径出现多类型则 `object` 包裹或按并集标 `string`（记录 `ambiguous` 注释）；字符串尝试识别 ISO-8601/枚举候选（≤6 去重值→enum 建议）；数组取元素合并递归；输出 `status=DRAFT` 模板。

### 7.4 发布路径

`PublisherEngine.publish`：权限/方向校验 → `x-codec: json`、`x-template`、`x-msg-id` 写入 headers →（有模板则 TemplateCodec 预校验，失败 4xxx）→ RateLimiter（Guava-free 令牌桶：`io.mngr.core.pub.RateLimiter`，双桶 msg/bytes）→ Adapter.publish → 失败分类：

```java
enum FailKind { RETRYABLE, NON_RETRYABLE, TIMEOUT, DISCONNECTED }
```

Kafka 按异常类型映射（`RetriableException`→RETRYABLE，`TopicAuthorizationException`→NON_RETRYABLE）；MQTT 按 CONNACK/异常映射。RETRYABLE：退避重试 ≤3 → BufferQueue（容量 10000，溢出 `DROP_OLDEST|DROP_NEWEST|BLOCK` 可配）→ 终败 `RuntimeRepository.appendDlq`。

---

## 8. Adapter 实现要点

### 8.1 mngr-adapter-kafka

- 每 Server 一个 `KafkaProducer`（`enable.idempotence=true` 默认开，`acks=all`，`max.in.flight=5`，`linger.ms=5`、`batch.size=64KB`，NFR-1 调优项全部映射自 `conn_json`）；
- 每订阅一个 `KafkaConsumer` + 专属 poll 线程（**不用** `ConsumerConfig` 的 multi-thread shared consumer，保证 pause/resume 与手动 ack 简单）：`enable.auto.commit=false`；手动模式：Handler 成功后 `commitAsync`（每 500ms 合并提交防高频），`ctx.ack()` 显式模式则攒 offset；
- `seek` 策略：`LATEST|EARLIEST|TIMESTAMP` 于 `ConsumerRebalanceListener.onPartitionsAssigned` 执行；
- Admin：懒创建单例 `AdminClient`，`createTopic/deleteTopic/listOffsets/consumerLag`（lag = `endOffset - committedOffset` 计算）；受 `allowAdminOps` 门控；
- DLQ 投递：`${topic}.DLQ`，headers 附加 `x-err-code/x-err-msg/x-orig-partition/x-orig-offset`。

### 8.2 mngr-adapter-mqtt（HiveMQ + Netty）

- `Mqtt5Client`（HiveMQ）建连参数直映：serverUrls 多节点、keepAlive、cleanStart、sessionExpiry、LWT、认证、TLS（Netty `SslContext`，支持 mTLS）；3.1.1 Server 用同一 client 的 `Mqtt3BlockingClient` API 分支；
- 订阅：`Mqtt5Subscribe` + 共享订阅拼 `$share/{group}/{topic}`（`sharedSubscriptionPrefix` 可配）；重连恢复清单交由 Core（§7.1）；
- 发布：QoS0/1/2 对应 `publish`（QoS1/2 等 PUBACK/PUBCOMP，成功回调携带 packetIdentifier）；`retain` 透传；`max_packet_size` 超限本地预检（读 CONNACK property）→ 明确错误 4003；
- MQTT5 user properties ↔ 统一 headers；**3.1.1 降级**：headers 序列化进 payload 信封 `{"_h":{...},"_p":{...}}`（发布可选、消费两种格式探测兼容）；
- Netty 线程数：`UseNativeTransport`（epoll，Linux）+ IO 线程 = `Math.max(2, cores/4)`，全部 Handler 立即 handoff 到 DispatchPool，禁止 IO 线程执行 Codec；
- 健康探测：MQTT5 PINGREQ（`ping()` API RTT）；`$SYS/broker/uptime` 采集仅只读开关 `enableSysMetrics`；
- FR-2.6 协议特性落点：`messageExpiryInterval`（M1x）随 publish 属性透传并在 `MessageContext.protocolMeta` 回显；`subscriptionIdentifier`（M2x）订阅注册时写入本系统 `subSid`，回程消息按该 ID 直达订阅（跳过通配匹配，同时可作为 FR-6.3 的 EXPLICIT 匹配信号）；`request()` 应答模式（M4x）用 `responseTopic=mnrrpc/{instanceId}` + 内存 `correlationId→CompletableFuture` 表实现；retained 清除（M8x）= `retain=true + 空 payload`，管理面清除走 §8.3 E7。

### 8.3 集群管理功能实现（FR-2.4 / FR-2.5）

#### 8.3.1 Core SPI 扩展

```java
// io.mngr.core.spi —— 与 TopicAdmin 并列的只读/受控管理面 SPI
public interface ClusterOpsGateway {                 // 每 Adapter 提供
    Capabilities capabilities();                     // 声明支持的子集（无凭据/版本不足时降级）
    ClusterOverview overview();                      // K1 / E1
    PageResult<ClientInfo> listClients(ClientQuery q);      // E3（Kafka 侧=空实现）
    // topic/partition/offset/acl/retained 等子操作按分组接口拆分：
    //   TopicOps(K2~K6,K10) GroupOps(K8,K9) AclOps(K11,E13) ReassignOps(K12~K14) TxOps(K15) QuotaOps(K16) RetainedOps(E7) SessionOps(E8) AlarmOps(E9)
}
```

写操作统一注解式门控：`@MgmtOp(risk = READ | WRITE | DANGER)`——`WRITE` 要求 `allowAdminOps=true`；`DANGER`（K6/K10/K13、E4/E8、`alterReplicaLogDirs`、发起重分配）额外要求请求携带 `confirm=<资源名>` 回显参数并写审计。所有调用经 `MgmtCallGuard`（每 Server 串行化 + 5s 超时 + 每分钟调用数限制）。

#### 8.3.2 KafkaOps（mngr-adapter-kafka）

K1~K18 与 `AdminClient` 方法一一映射（无二次封装层）：

| 功能 | 实现 |
|------|------|
| K1 集群概览 | `describeCluster()` → nodes/controller/clusterId；`describeFeatures()`（K17）合并展示 |
| K2~K5 | `listTopics/describeTopics(includeInternal)、create/deleteTopics、createPartitions、incrementalAlterConfigs(TOPIC)` |
| K6~K7 | `incrementalAlterConfigs(BROKER / BROKER_LOGGER)`（DANGER） |
| K8~K9 | `list/describe/deleteConsumerGroups、listOffsets、allCommittedOffsets、deleteOffsets(3.8+，低版本 capabilities 声明不支持)` |
| K10 | `deleteRecords`（DANGER） |
| K11 | `describe/create/deleteAcls`，资源模型 `AclBindingFilter` 表单化（principal/host/operation/permission） |
| K12~K14 | `list/alterPartitionReassignments、electLeaders、describe/alterReplicaLogDirs`（查询 P1、执行 P2/DANGER） |
| K15~K16 | `list/describe/abortTransactions、describe/alterClientQuotas` |
| K18 lag 矩阵 | `listOffsets(end) - allCommittedOffsets` 组合计算，5s 缓存，按 lag 降序 TopN |

#### 8.3.3 EmqxMgmtClient（mngr-adapter-mqtt）

- HTTP 客户端：Spring 6 `RestClient`（同步即可，管理面低频），Base = `mgmtApi.baseUrl`（如 `http://emqx:18083/api/v5`）；
- 认证：先 `POST /login {name, password}`（apiKey/apiSecret，来自 `cred_enc` 解密）换取 JWT，**有效期 90s，剩余 <10s 自动续签**；401 单次重登重试；
- 路由映射：E1 `/nodes`、E2 `/broker` + `/metrics/{node}`、E3/E4 `/clients*`、E5 `/subscriptions*`、E6 `/topics`、E7 `/retained`（`DELETE /retained/{topic}` 注意 topic 需 URL 编码）、E8 `/sessions*`、E9 `/alarm`、E10 `/listeners`、E11 `/rate_limit`、E12 `/authentication/{listener}/{id}users`、E13 `/authorization/sources/{id}`、E14 `/banned`；
- 版本兼容：启动时 `GET /api/v5/status` 探测 EMQX 版本，4.x 集群关闭 `mgmtApi` 并将 capabilities 全部置灰（前端展示"请配 API 凭据/版本不支持"，对应需求 `FEATURE_DISABLED` 语义）；
- 未配置凭据时 `EmqxMgmtGateway` 仍注册但 `capabilities().mgmtApi=false`，E1/E2 降级走 `$SYS` 只读采集。

#### 8.3.4 REST 端点（mngr-server 增量）

```
GET   /api/v1/servers/{id}/cluster                 K1/E1 概览
GET   /api/v1/servers/{id}/brokers                 Kafka broker 列表+配置
POST  /api/v1/servers/{id}/topics/_create          {name,partitions,replication,configs}   [WRITE]
POST  /api/v1/servers/{id}/topics/{t}/_add-partitions   {count}                        [WRITE]
PATCH /api/v1/servers/{id}/topics/{t}/_configs     {ops:[{key,op,value}]}               [WRITE]
DELETE /api/v1/servers/{id}/topics/{t}             confirm={t}                          [DANGER]
GET   /api/v1/servers/{id}/consumer-groups?state=  K8；GET .../consumer-groups/{g}/lag  K18
DELETE /api/v1/servers/{id}/consumer-groups/{g}    [WRITE]
POST  /api/v1/servers/{id}/consumer-groups/{g}/_reset-offsets  {to:latest|earliest|@ts} [DANGER]
GET/POST/DELETE /api/v1/servers/{id}/acls          K11/E13 [WRITE]
GET   /api/v1/servers/{id}/reassignments           K12 只读(P1)
GET   /api/v1/servers/{id}/clients?q=&page=        E3
DELETE /api/v1/servers/{id}/clients/{clientId}     E4 踢下线, confirm={clientId}         [DANGER]
GET   /api/v1/servers/{id}/subscriptions           E5（与本地登记对账 diff 视图数据源）
GET/DELETE /api/v1/servers/{id}/retained[/{topic}] E7 [WRITE for DELETE]
GET   /api/v1/servers/{id}/nodes /metrics /alarm   E1/E2/E9
```

#### 8.3.5 前端"集群管理"页（mqtt-mngr-ui 增量）

`views/cluster/`：Server 选择器（协议自适应 Tab）——Kafka Tab：概览卡片（broker/controller）、Topics 表格（创建/扩容/配置抽屉、删除二次确认输入名）、消费组表格（成员/分配/lag 热力矩阵，重置与删除）、ACL 面板；EMQX Tab：节点卡片、客户端表格（搜索 + 踢下线）、订阅对账视图（本地登记 × Broker 实际，diff 高亮）、Retained 浏览器（topic 搜索 + 查看 payload + 删除）。DANGER 操作统一走 `DangerConfirmDialog`（输入资源名 + 红色警示），能力缺失项按 `capabilities()` 置灰并 tooltip 说明原因。

---

## 9. 前端设计（mqtt-mngr-ui，Vue 3）

### 9.1 技术结构

```
mqtt-mngr-ui/
├── vite.config.ts        # dev proxy /api、/ws → localhost:8080
├── src/
│   ├── api/              # axios 实例(token注入/错误码toast) + 资源API模块
│   │   └── ws.ts         # 单例连接管理器：action帧封装、心跳、断线指数退避重连
│   ├── stores/           # Pinia: servers.ts topics.ts templates.ts realtime.ts(消息流环形缓冲2000条)
│   ├── router/           # 路由即菜单
│   ├── views/
│   │   ├── dashboard/    # 状态总览：Server健康卡片、TPS/lag 趋势(ECharts,WS推送)
│   │   ├── server/       # Server列表/表单(按 protocol 动态schema表单)/连接操作
│   │   ├── topic/        # Topic映射管理、通配模式提示
│   │   ├── cluster/      # 集群管理（§8.3.5）：Kafka Tab(broker/topics/消费组lag矩阵/ACL)
│   │   │                 #   EMQX Tab(节点/客户端/订阅对账/retained/告警)，危险操作统一二次确认
│   │   ├── template/     # 模板CRUD：字段表格编辑(嵌套树)、样例反推向导、
│   │   │                 #   匹配预览面板、版本diff视图、生成代码下载
│   │   ├── messaging/    # 收发调试台：左发布器(模板选择→自动填表单/JSON编辑器)、
│   │   │                 #   右订阅实时流(表格按 matchType 着色, JSON展开, 可导出)
│   │   ├── dlq/          # DLQ浏览/重放按钮
│   │   └── settings/     # token、存储模式显示、关于
│   └── components/       # JsonViewer HeaderEditor StatusBadge PageTable
```

### 9.2 交互约定

- 统一响应拦截：`code!=0` → ElMessage.error(`code: message`)，`requestId` 可复制（与后端日志关联）；
- 实时流页面通过 WS `subscribe` 帧复用后端**调试通道**（不产生正式订阅记录）；模板匹配列展示 `templateName / matchType / score`，实现需求"消息快速归类"的可视化验证；
- 危险操作（删 Server force、不兼容模板变更）二次确认弹窗需输入资源 id 片段；
- 前端不打包 sourcemap 进发布产物；构建产物由 `frontend-maven-plugin` 执行 `npm ci && npm run build` 后拷贝至 `mngr-server/src/main/resources/static/`（profile `with-frontend` 控制，纯后端 CI 可跳过）。

---

## 10. 横切设计

| 主题 | 方案 |
|------|------|
| 配置 | `application.yml` + 环境变量覆盖（`MNGR_*` relaxed binding）；数据目录 `mngr.data-dir` 默认 `./data` |
| 日志 | Logback JSON encoder（生产）/ 彩色 pattern（本地）；消息级日志 `mngr.log.sample-rate=0.01`；敏感字段 Logback converter 脱敏 |
| 指标命名 | `mngr_publish_total{server,topic,result}`、`mngr_consume_latency_seconds`、`mngr_kafka_lag{group,topic}`、`mngr_template_match_total{template,matchType}`、`mngr_buffer_depth{server}`、`mngr_dlq_total` |
| 事件总线 | Core `EventListenerRegistry`（进程内同步广播，server 模块转 WS 帧与指标） |
| 时钟/ID | messageId = UUIDv7（`Uuid7.next()`）；时间统一 UTC 存储、展示层本地化 |
| 单测 | Core 覆盖 mock `ProtocolAdapter`/`ConfigRepository`（file 内存实现）；Codec/Inferer/Matcher 为纯函数，覆盖率重点（NFR-9） |
| 集成测试 | testcontainers `confluentinc/kafka` + `emqx/emqx:5.8`；`MngrTestApp` 起真实 pipeline，收发断言（含 §7 异常场景 1/4/11/12） |

## 11. 部署与运行

```bash
# 构建（含前端）
mvn -Pwith-frontend clean package -DskipTests
# 运行
MNGR_MASTER_KEY=*** MNGR_AUTH_TOKEN=*** \
java -jar mngr-server/target/mngr-server.jar \
     --mngr.store.mode=h2 --server.port=8080 --mngr.data-dir=/var/lib/mngr
# 浏览器访问 http://host:8080/  （管理台）
# CLI 共用 data 目录（只读模式）或经 REST：mngr-cli --server-url http://host:8080 --token ***
```

Docker：`eclipse-temurin:17-jre` 单层 + volume 挂 `/var/lib/mngr`；健康检查 `/actuator/health`；资源限制建议 2C4G（SDK 内嵌模式按其依赖裁剪，不依赖 Spring Web）。

## 12. 需求 → 模块追溯矩阵（摘要）

| 需求 | 落点 |
|------|------|
| FR-1 Server 管理/重连/健康 | `core.server.*` + Adapter connect + §7.1 状态机；REST `ServerController`；UI `views/server` |
| FR-2.1~2.3 Topic 管理/本地映射/共享订阅 | `core.topic.*`、`adapter-kafka.admin`、`adapter-mqtt` share 拼接；`TopicController`；UI `views/topic` |
| FR-2.4 Kafka 管理清单（K1~K18） | §8.3.2 `KafkaOps` + `ClusterOpsGateway`；`KafkaMgmtController`；UI `views/cluster` Kafka Tab |
| FR-2.5 EMQX 管理清单（E1~E14） | §8.3.3 `EmqxMgmtClient`；`EmqxMgmtController`；UI `views/cluster` EMQX Tab |
| FR-2.6 协议消息级特性（M*x） | §8.2 尾部落点（expiry/subscriptionId/rpc/retained 清除） |
| FR-3 发布/限速/缓冲/DLQ | `core.pub.*`（§7.4）；`PublishController`；UI `views/messaging` |
| FR-4 订阅/背压/重试/DLQ | `core.sub.*`（§7.2）；`SubscriptionController`；DLQ 表 + `DlqController` |
| FR-5 JSON Codec | `mngr-codec-json`（信封兼容、指纹、unknown 策略） |
| FR-6 消息模板 | `core.template.*`（§7.3）、`msg_template` 表、UI `views/template` |
| FR-7 指标/日志/审计/CLI | `core.metrics`、logback、`audit_log`/jsonl、`mngr-cli` |
| NFR-4 安全 | AES-GCM `CredCipher`、Token Filter、日志脱敏 converter、TLS 配置 |

## 13. 遗留事项（开发前需确认）

1. MQTT3.1.1 的 headers 信封方案是否可接受（否则 3.1.1 仅支持无 header 消息）；
2. 管理台是否需要多用户/权限分级（当前单 Token 全局权限，审计 `actor` 固定 `token-holder`）；
3. H2 `MODE=MySQL` 兼容度是否满足后续迁移 PostgreSQL 计划（需求 §FR-7.4 V1.1 中心存储）；
4. CLI 与守护进程共享 data 目录时的文件锁竞争——默认建议 CLI 走 REST（`--server-url`），本地模式仅单进程场景；
5. EMQX API Key/Secret 的轮转流程（Dashboard 侧生成 → 本系统凭据更新）需运维手册约定；JWT 90s 有效期已在 §8.3.3 处理，但 EMQX 侧关闭 API 访问时的降级提示文案待定稿；
6. Kafka `deleteOffsets`（KIP-802）依赖集群 3.8+，低版本集群的能力探测结果需在管理台明示"不支持"；FR-2.7 范围边界如有业务突破（如需要规则引擎），走需求变更评审。
