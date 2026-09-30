# mqtt-mngr 控制台 —— 业务 UI 设计说明书（高保真）

| 项目 | 内容 |
|------|------|
| 文档版本 | V1.0 |
| 编写日期 | 2026-09-30 |
| 上游文档 | [UI设计规范 v1.3](./UI设计规范及单页案例/UI设计规范.md)（令牌/组件/图标/a11y 一律遵循，本文档只定义业务层扩展）、[系统技术设计说明书 §9](./kafka-emqx-client-系统技术设计说明书.md) |
| 高保真原型 | [../design/hifi/index.html](../design/hifi/index.html)（浏览器直接打开，含深浅主题切换与页面导航） |

> 本文档为通用规范 §12 所要求的"业务 UI 文档"：定义信息架构、页面布局、业务组件与交互文案；不重复定义令牌。**所有色值/字号/间距/圆角/阴影/动效均引用规范 §2 令牌，禁止硬编码。**

---

## 1. 信息架构与导航

### 1.1 侧栏导航（228px 标准玻璃）

```
概览
  ├─ 监控总览          /dashboard
消息接入
  ├─ 服务连接(Server)   /servers
  ├─ Topic 管理         /topics
  ├─ 集群管理           /cluster      (Kafka Tab / EMQX Tab，随所选 Server 协议自适应)
消息工厂
  ├─ 消息模板           /templates
  ├─ 收发调试台         /messaging
运维
  ├─ 死信 DLQ          /dlq
  └─ 系统设置           /settings
```

- 顶栏（60px 强玻璃）：品牌区「MQTT 消息管理台 · Mngr」+ 全局搜索（server/topic/模板，Ctrl+K）+ 主题切换 + 通知（DLQ/告警计数角标）+ 头像；
- 面包屑跟随侧栏层级；页面标题右侧为页面级主操作按钮。

### 1.2 路由与状态约定

- 页面间跳转保持所选 Server 上下文（Pinia `useScopeStore`），集群管理/Topic/调试台共享同一 `activeServerId`，侧栏对应项高亮；
- 空态规范：图标(24px 线性) + 说明 + 主操作按钮（规范 §8"空状态可行动"），如"暂无 Server，点击「注册 Server」开始接入"。

---

## 2. 业务组件扩展（在通用组件之上）

| 组件 | 形态（引用令牌） | 语义映射 |
|------|------------------|----------|
| `StatusBadge` | 圆点 `dot` + Tag 变体 | INIT→`tag--info`；CONNECTING→`tag--warning`（点做 1.2s 呼吸动画）；CONNECTED→`tag--success`；DEGRADED→`tag--warning`；DOWN→`tag--danger` |
| `ProtocolTag` | Kafka：`tag--primary` + layers 图标；MQTT：`tag--primary` 描边版 + radio 图标 | 协议图标统一 Lucide 线性，禁 emoji（规范 §5） |
| `MatchBadge` | 模板匹配类型徽标 | EXPLICIT→`tag--primary`；BOUND→`tag--success`；SIMILARITY→`tag--warning`（附得分 `type.tiny`）；NONE→`tag--info` "未归类" |
| `LagHeatmap` | 组×分区热力格，色阶：0→`surface`、<1k→`color.primary-bg`、<100k→`color.warning/.15`、≥100k→`color.danger/.15`，数字 `tabular-nums` | 附图例，不单靠颜色（a11y §6） |
| `JsonViewer` | 等宽字体 `font.mono` 13px，键 `color-primary`、串 `success`、数 `accent`、注释 `text-3`，折叠三角 16px | 超过 200 行虚拟滚动 |
| `TemplateFieldTable` | 实体表面树形表格：字段名(mono)/类型下拉/必填星/注释/示例，缩进 20px/层 | 行 hover 显示"加子字段"文本按钮 |
| `DangerConfirmDialog` | 模态 480px 强玻璃 + `shadow.lg`；标题 danger 图标；**需输入资源 id 才解锁主按钮**；展示影响说明 | 对应技术设计 §8.3.1 DANGER 级 |
| `LiveFeed` | 消息实时流列表（调试台右栏）：时间戳 tiny + topic(mono) + MatchBadge + 摘要行；点击行展开 JsonViewer；新消息顶部插入 + spring 入场 320ms | WS 断线时顶部 warning 内嵌条"推流已断开，重连中…" |
| `ConnForm` | Server 注册抽屉内**按协议动态渲染**的表单组（Kafka: bootstrap/SASL/TLS；MQTT: hosts/clientId 模板/QoS/keepAlive/API 凭据） | 敏感字段密码框 + "已加密存储" caption |
| `MetricSpark` | 32px 高迷你折线/柱状（可视化色板序号 1/4） | 涨红跌绿规则不适用（非行情），统一 primary |

---

## 3. 逐页高保真设计

### 3.1 监控总览 `/dashboard`

```
[4×统计卡(玻璃): 在线Server 5/6 · 发布TPS 8,420 · 消费TPS 7,960 · 未消化Lag 24.3万]
[Server 健康卡片栅格(3列)：每卡 名称+协议+StatusBadge+TPS sparkline+重连次数]
[两栏: 左·吞吐趋势大图(发布/消费双系列, 色板1/2)  右·模板归类环图+TopN表]
[底部: 最近告警/DLQ 流(表格 10 行)]
```

- 统计卡数字 `24px/600/tabular-nums`，副趋势 `type.tiny`；
- 模板环图数据源 = FR-7.1 按模板维度指标，扇区外标注 `matchType` 占比。

### 3.2 服务连接 `/servers`

- 工具栏：搜索、协议筛选、状态筛选、[导入/导出]、**+ 注册 Server**（主按钮，渐变+glow）；
- 表格列：名称/ID、协议、地址(mono 截断+tooltip)、连接模式、状态、重连数、更新时间、操作（详情 · 连接/断开 · 编辑 · 删除）；
- `删除` 若被 Topic/订阅引用 → 弹 DangerConfirm（列出引用清单，需输入 serverId，`force` 复选框默认不可用需勾选"我已了解风险"）；
- 注册抽屉 480px：基础信息 → 协议动态参数 →（Kafka）安全页签 / （MQTT）会话与遗嘱页签 → 底部 [测试连接]（返回 spinner→success/error toast，不关抽屉）。

### 3.3 Topic 管理 `/topics`

- 工具栏含"当前 Server 上下文"下拉；列表：topicId、名称/模式(mono，通配高亮 `+ #`)、方向、格式、QoS、**默认模板**（MatchBadge 风格）、引用计数、操作；
- 行展开子表：该 Topic 关联的订阅清单（组、状态、lag）；
- Kafka 且 `allowAdminOps=true` 时行操作出现"Broker 详情"（跳转集群管理对应 Tab）。

### 3.4 集群管理 `/cluster`

- 页头 Tab：**Kafka**（概览|Topics|消费组|ACL）/ **EMQX**（节点|客户端|订阅对账|Retained|告警）；
- Kafka-概览：broker 卡片（id/地址/控制器徽标）+ 集群元数据行（clusterId/KRaft/版本）；
- Kafka-Topics：表格（分区数/副本/retention/内部标识），行操作按风险分级：查看配置(WRITE 表单抽屉)、扩容分区(WRITE)、删除(**DANGER**)；
- Kafka-消费组：左列表+右详情（成员表、分配矩阵 = `LagHeatmap`、[重置 offset](DANGER，选择器：latest/earliest/时间戳) [删除空组](WRITE)）；
- EMQX-客户端：搜索 + 表格（clientId/IP/协议版本/keepalive/连接时长/订阅数），行操作"断开连接"= DANGER；能力缺失（无 API 凭据）时整页置灰提示卡（规范 §4.7 Empty 变体 + tooltip "未配置 EMQX 管理 API"）；
- EMQX-订阅对账：三栏 diff 视图——本系统登记 / Broker 实际 / 差异高亮（仅本地=warning、仅 Broker=info、双向=success）；
- EMQX-Retained：topic 搜索 + payload 预览(JsonViewer) + 按 topic 删除（DANGER）。

### 3.5 消息模板 `/templates`

```
左 280px 模板列表(搜索/新建)      右 编辑区
┌────────────────────────────────────────────┐
│ 名称 · 版本v3 [当前] 状态Tag  引用:2 Topic │ ← 头部行 + [反推][导入Schema][导出][生成代码▾]
│ Tabs: 字段定义 | 匹配预览 | 版本历史        │
│ 字段定义: TemplateFieldTable + [添加字段]   │
│ 匹配预览: 左样例JSON编辑器 右候选模板得分条 │
│   (progress 条宽=score, 阈值线0.8 虚线)     │
│ 版本历史: diff 列表, 不兼容变更红 Tag       │
└────────────────────────────────────────────┘
```

- "样例反推"向导（模态 3 步，stepper 用 `type.caption`）：粘贴样例 → 推断结果编辑（ambiguous 字段 warning 标注）→ 保存为草稿；
- 保存时若检测不兼容变更 → 弹对比框（左旧右新 diff）+ 要求勾选"确认影响 N 个订阅"；
- 字段行必填缺失时行左侧 danger 竖线 2px。

### 3.6 收发调试台 `/messaging`

- 左右双栏 480 : 自适应；顶部共享 Server/Topic 选择条；
- 左·发布器：格式切换（模板表单 ⇄ JSON 源码），选中模板时表单按字段生成（必填 `*`、注释为 placeholder、示例为提示），headers 键值行编辑器，QoS/retain 控件（MQTT）或 key 输入（Kafka），**[发送]** 主按钮；结果卡：成功→offset/packetId 或失败→错误码 + "进 DLQ"链路；
- 右·实时流：工具条（订阅本 Topic 调试流开关、暂停、清空、导出）；`LiveFeed` 行内显示 `topic · MatchBadge · 摘要`；展开含原始/解析后双 JSON；
- 推流为 WS 调试通道，页面顶部 info 条注明"调试订阅不计入正式订阅"。

### 3.7 死信 DLQ `/dlq`

- 筛选：server/topic/错误码/时间范围；表格列：时间、topic、模板（或"未归类"）、错误码+原因（hover 全文）、大小、操作（查看 · 重放）；
- 批量：勾选 → 主色浅底批量条"重放选中 N 条"（重放=以原 headers 发布回目标 topic，成功计数 toast）；
- file 存储模式下查询页签提示"当前为无库模式，仅支持尾部扫描/导出"（能力降级显式化）。

### 3.8 系统设置 `/settings`

- 分区卡：存储模式（h2/file 只读展示 + 能力清单）、鉴权 Token 状态、主密钥来源、数据目录、指标端点链接、版本与许可证；
- 危险区：DLQ 保留天数（表单）、审计导出。

---

## 4. 状态色 / 语义映射总表

| 语义 | 组件表达 |
|------|----------|
| 在线/成功 | `tag--success` + 绿点 |
| 连接中/降级/警告 | `tag--warning`（连接中带呼吸点） |
| 离线/错误/危险操作 | `tag--danger`、`btn--danger` |
| 未归类/中性 | `tag--info` |
| 主数据/选中/协议 | `tag--primary` |
| 趋势涨跌 | 不适用行情红绿规则；增长统一 primary 文字 + trend 图标 |

## 5. 文案细则（承接通用规范 §8）

- 按钮："注册 Server"“发送”“重放选中 3 条”（动词+对象+具体数字）；
- 连接测试结果："已连接 kafka-prod（3 broker · 3.7.2）"，失败："认证失败（SASL PLAIN），请检查用户名/密码"——禁止"操作成功/失败"；
- DANGER 弹窗标题："删除 Topic order-paid？"，副文案列出分区数/消息保留期/引用订阅数量。

## 6. 实现落地约束

1. 令牌文件置于 `src/styles/tokens/`，CSS 变量前缀 `--wb-`；Element Plus 变量映射按通用规范 §10；
2. 原型 HTML 即视觉验收基准：`design/hifi/index.html` 中每个页面的结构、组件形态、间距与本文档一致，开发时以 Vue SFC 复刻；
3. 图标全部 Lucide 线性 SVG（原型内置 symbol 集），业务代码经 `<AppIcon>` 封装；
4. 图表（趋势/环图/热力）用 ECharts，色板顺序取通用规范 §2.9；
5. 断点：<1024px 侧栏收起为图标栏；<768px 调试台上下堆叠（基线仍为 ≥1280px 桌面）。
