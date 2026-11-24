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

---

# 关于作者

> 源码猿（master336）—— 一个把「踩过的坑」写成「别人脚下的路」的技术作者。

## 一句话介绍

**源码猿（master336）**，现就职于**某公司**，担任**高级技术架构师 / 产品售前专家**（腾讯云开发者社区认证）。长期深耕 Java 后端与分布式架构，近年将主要精力投向 **AI Agent、大模型私有化部署与工程化落地**，坚持用「能跑通、可复现、讲原理」的标准写每一篇文章。

## 身份与平台

| 平台 | 主页 | 身份 / 荣誉 |
| --- | --- | --- |
| CSDN 博客 | [master336.blog.csdn.net](https://master336.blog.csdn.net) | 人工智能领域**新星创作者**，原力等级 **298**，累计获赞 **461** 次、评论 **86** 条 |
| 腾讯云开发者社区 | [cloud.tencent.com/developer/user/1558800](https://cloud.tencent.com/developer/user/1558800) | 认证作者 · 某公司｜高级技术架构师、产品售前专家 |

## 技术视野

作者的写作版图横跨「从底层到应用、从传统工程到 AI 原生」的完整链路：

- **后端与架构**：Java、Spring / SpringBoot / SpringCloud、微服务架构、Maven
- **数据与存储**：MySQL、Redis、PostgreSQL、Greenplum、GaussDB、ETL 与大数据处理
- **运维与基础设施**：Linux、Nginx、操作系统、网络协议（TCP/UDP、HTTP/HTTPS、OSI 七层模型）、RabbitMQ
- **前端与客户端**：JavaScript、CSS、Vue.js、HTML、Android、微信小程序
- **AI 与智能化**：机器学习、NLP、图像处理、数据分析、AIGC
- **AI 工程化（近期主线）**：AI Agent 架构与多智能体协同、Ollama / ComfyUI 私有大模型部署、MCP 协议、Skill 资产化、大模型评测与上下文召回

## 写作风格

不同于「Hello World 式」的速食教程，作者的文章有三个鲜明特点：

1. **讲透原理** —— 不满足于告诉你「怎么做」，更在意解释「为什么这么做」；
2. **给出可落地的工程** —— 完整目录结构、核心代码、硬件选型、避坑清单，拿来就能改；
3. **有数据、有出处** —— 明确区分哪些是实测、哪些是预测、哪些不可横向比较，拒绝拍脑袋结论。

近期在腾讯云开发者社区连续产出《用 CodeArts 把一个自托管云相册真正做完》《Ollama 私有大模型部署与生态配置完全指南》《从零构建 AI Agent：一份写给工程师的深度开发指南》《一次讲透 HTTP 与 HTTPS》等长文，几乎保持日更节奏。

## 联系方式

- CSDN：[https://master336.blog.csdn.net](https://master336.blog.csdn.net)
- 腾讯云开发者社区：[https://cloud.tencent.com/developer/user/1558800](https://cloud.tencent.com/developer/user/1558800)

---

## ☕ 支持作者

看到这里，说明你已经陪我读到了文章的最后。

如果这些文字曾经帮你**少踩一个坑、少熬一个夜、少走一次弯路**，那它就是有价值的。写作这件事，靠的不只是兴趣，还有一台够用的机器、几个付费的实验环境、以及愿意花在查证与实测上的时间。

**你的每一份支持，都会变成下一篇文章里的一个真实实验、一次更深入的拆解。**

> 不用有压力 —— 一杯咖啡的钱，就足够让我开心很久。
> 当然，一个点赞、一条评论、一次转发，同样是最好的鼓励。

### 打赏渠道

<div align="center">

| 微信支付 | 支付宝 |
| :---: | :---: |
| ![微信支付二维码](images/wx.png) | ![支付宝二维码](images/zfb.jpg) |
| *推荐使用微信支付* | *支持信用卡 / 花呗付款* |

</div>

### 不打烊的支持方式

如果不方便打赏，下面这些同样珍贵：

- 👍 **点赞** —— 让更多人看到这篇内容
- ⭐ **收藏** —— 下次要用时能第一时间找到
- 💬 **评论** —— 你的问题常常是下一篇选题的起点
- 🔗 **转发** —— 把有用的东西分享给需要的朋友

感谢每一位读到这里的人。我们下篇文章见。

---

*Made with ❤️ by 源码猿（master336）*
