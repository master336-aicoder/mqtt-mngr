# mqtt-mngr

> Kafka & EMQX 双协议统一消息管理客户端

一站式管理 Kafka 和 MQTT(EMQX) 消息中间件的服务连接、Topic、消息模板、订阅、发布、死信重放及集群运维。内建 RBAC 权限体系，嵌入式 H2 数据库零依赖部署。

---

## 功能特性

| 模块 | 说明 |
|------|------|
| **服务连接** | Kafka / MQTT 双协议接入，连接状态实时监控 |
| **Topic 管理** | 通配符订阅、Broker Topic 浏览 |
| **消息模板** | JSON 结构自动推断、版本管理、模式匹配 |
| **收发调试** | 在线发布消息，支持模板填充 |
| **订阅管理** | 启动 / 停止 / 暂停 / 恢复，失败自动重试 |
| **死信 DLQ** | 失败消息查询、批量重放 |
| **集群管理** | Kafka 集群（Topic / ConsumerGroup / 分区）+ EMQX 集群（节点 / 客户端 / 订阅） |
| **RBAC 权限** | 用户、角色、菜单权限管理，Session Token 鉴权 |
| **审计日志** | 全操作审计，可追溯 |
| **监控指标** | Prometheus 指标暴露，Actuator 健康检查 |

---

## 技术栈

| 层 | 技术 |
|----|------|
| 后端 | Java 21 · Spring Boot 3.3.5 · MyBatis-Plus 3.5.7 |
| 数据库 | H2 (文件模式，MySQL 兼容) |
| 协议 | Kafka Client 3.7.1 · HiveMQ MQTT Client 1.3.3 |
| 前端 | Vue 3 (全局构建) · 原生 CSS |
| 安全 | BCrypt 密码 · AES-256-GCM 凭据加密 · Session Token |
| 监控 | Micrometer + Prometheus · Spring Boot Actuator |

---

## 快速开始

### 环境要求

- JDK 17+（推荐 21）
- Maven 3.8+

### 编译启动

```bash
# 编译打包
bin/build.sh --skip-test

# 启动服务
bin/start.sh

# 访问
# 前端:  http://localhost:8090
# 健康:  http://localhost:8090/actuator/health
# 指标:  http://localhost:8090/actuator/prometheus
```

### 默认账号

| 用户名 | 密码 | 角色 |
|--------|------|------|
| `admin` | `admin123` | 超级管理员 |

> 首次部署后请立即修改默认密码。

### Windows

```cmd
bin\build.bat --skip-test
bin\start.bat
```

---

## 项目结构

```
mqtt-mngr/
├── bin/                  # 运维脚本（启停/巡检/打包）
├── doc/                  # 设计文档与手册
│   ├── 运维手册.md
│   ├── 二次开发手册.md
│   ├── 需求设计说明书.md
│   ├── 系统技术设计说明书.md
│   └── UI设计说明书.md
├── src/main/java/io/mngr/
│   ├── config/           # 配置层
│   ├── core/             # 核心领域（SPI / 连接 / 发布 / 订阅 / 模板）
│   ├── store/            # 存储层（H2 / File）
│   ├── adapter/          # 协议适配器（Kafka / MQTT）
│   ├── web/              # REST 控制器 + 认证 + 审计
│   ├── sys/              # RBAC（用户 / 角色 / 菜单 / 会话）
│   └── ws/               # WebSocket
└── src/main/resources/
    ├── application.yml   # 主配置
    ├── schema.sql        # 建表 + 种子数据
    ├── logback-spring.xml
    └── static/           # 前端 SPA
```

---

## 运维脚本

| 脚本 | 用途 | 用法 |
|------|------|------|
| `bin/build.sh` | 编译打包 | `bin/build.sh [--skip-test] [--clean]` |
| `bin/start.sh` | 启动服务 | `bin/start.sh [--foreground]` |
| `bin/stop.sh` | 停止服务 | `bin/stop.sh [--force]` |
| `bin/restart.sh` | 重启服务 | `bin/restart.sh` |
| `bin/health.sh` | 健康检查 | `bin/health.sh` |
| `bin/inspect.sh` | 全面巡检 | `bin/inspect.sh` |

环境变量：`MNGR_PORT`、`MNGR_DATA_DIR`、`MNGR_MASTER_KEY`、`MNGR_AUTH_TOKEN`、`MNGR_JVM_OPTS`

---

## API 概览

共 75 个 REST 端点，覆盖 13 个控制器：

| 模块 | 端点数 | 基础路径 |
|------|--------|----------|
| 认证 | 3 | `/api/v1/public/*`、`/api/v1/user/info` |
| 用户管理 | 7 | `/api/v1/users` |
| 角色管理 | 6 | `/api/v1/roles` |
| 菜单管理 | 4 | `/api/v1/menus` |
| 服务连接 | 7 | `/api/v1/servers` |
| Topic | 5 | `/api/v1/topics` |
| 消息模板 | 9 | `/api/v1/templates` |
| 消息发布 | 1 | `/api/v1/publish` |
| 订阅管理 | 8 | `/api/v1/subscriptions` |
| 死信 DLQ | 5 | `/api/v1/dlq` |
| 状态/审计 | 2 | `/api/v1/status` |
| Kafka 管理 | 10 | `/api/v1/mgmt/kafka/{serverId}` |
| EMQX 管理 | 8 | `/api/v1/mgmt/emqx/{serverId}` |

---

## 文档索引

| 文档 | 说明 |
|------|------|
| [运维手册](doc/运维手册.md) | 部署、配置、巡检、备份、监控、故障排查 |
| [二次开发手册](doc/二次开发手册.md) | 架构、分层、新增功能指南、编码规范 |
| [需求设计说明书](doc/kafka-emqx-client-需求设计说明书.md) | 功能需求、用户故事 |
| [系统技术设计说明书](doc/kafka-emqx-client-系统技术设计说明书.md) | 架构设计、模块划分、接口定义 |
| [UI 设计说明书](doc/kafka-emqx-client-UI设计说明书（高保真）.md) | UI 设计规范、交互说明 |

---

## 安全提醒

- **生产部署必须修改** `mngr.master-key`（凭据加密主密钥）
- 首次登录后修改 admin 默认密码
- 不要将未鉴权的服务暴露到公网
- 危险操作（删除 Topic、重置偏移等）需开启 `mngr.allow-admin-ops` 并携带 `confirm` 参数

---

## License

Private · All rights reserved.
