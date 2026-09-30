/* mqtt-mngr 前端 SPA · Vue3 全局构建 · 无打包器
   对接 /api/v1/* REST 与 /ws WebSocket，视觉复刻 design/hifi/index.html（验收基准）。 */
(function () {
  const TOKEN_KEY = 'mngr-token';

  async function req(method, path, opt) {
    opt = opt || {};
    let url = '/api/v1' + path;
    if (opt.q) {
      const p = new URLSearchParams();
      Object.entries(opt.q).forEach(([k, v]) => { if (v !== '' && v != null) p.set(k, String(v)); });
      const s = p.toString();
      if (s) url += '?' + s;
    }
    const h = {};
    if (opt.body !== undefined) h['Content-Type'] = 'application/json';
    const tk = localStorage.getItem(TOKEN_KEY) || '';
    if (tk) h['X-Mngr-Token'] = tk;
    const r = await fetch(url, { method, headers: h, body: opt.body !== undefined ? JSON.stringify(opt.body) : undefined });
    let j;
    try { j = await r.json(); } catch (e) {
      throw Object.assign(new Error('HTTP ' + r.status + ' 非 JSON 响应'), { code: -1 });
    }
    if (j.code !== 0) {
      const err = new Error((j.message || 'error') + ' (' + j.code + (j.requestId ? ', ' + String(j.requestId).slice(0, 8) : '') + ')');
      err.code = j.code; err.http = r.status; err.raw = j;
      throw err;
    }
    return j.data;
  }

  function parseTs(v) {
    if (v == null) return null;
    if (typeof v === 'number') return v < 1e11 ? v * 1000 : v;
    const d = new Date(v);
    return isNaN(d.getTime()) ? null : d.getTime();
  }
  function fmt(v) {
    const t = parseTs(v);
    if (!t) return v == null ? '—' : String(v);
    const d = new Date(t);
    const p = n => String(n).padStart(2, '0');
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate())
      + ' ' + p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  }

  const META = {
    dashboard: ['概览 <span>/</span> <span>监控总览</span>', '监控总览', '实时掌握 Server 连接健康、订阅与死信情况（数据来自 /status/overview）。'],
    servers: ['消息接入 <span>/</span> <span>服务连接</span>', '服务连接 (Server)', '注册、连接与监控 Kafka / EMQX Broker，凭据 AES-256-GCM 加密且永不回显。'],
    topics: ['消息接入 <span>/</span> <span>Topic 管理</span>', 'Topic 管理', '维护 Topic 映射、方向、默认格式与消息模板绑定（BOUND 归类依据）。'],
    cluster: ['消息接入 <span>/</span> <span>集群管理</span>', '集群管理', 'Broker 侧 Topic / 消费组 / 客户端 / Retained 管理（受 allowAdminOps 门控，DANGER 需回显资源名）。'],
    templates: ['消息工厂 <span>/</span> <span>消息模板</span>', '消息模板', '字段结构定义、样例反推、匹配预览与版本演进（不兼容变更 6002 拦截）。'],
    messaging: ['消息工厂 <span>/</span> <span>收发调试台</span>', '收发调试台', '发布消息并实时观察订阅流的模板归类结果；管理正式订阅。'],
    dlq: ['运维 <span>/</span> <span>死信 DLQ</span>', '死信 DLQ', '浏览解析/发送失败的消息，查看原因并重放回目标 Topic。'],
    settings: ['运维 <span>/</span> <span>系统设置</span>', '系统设置', '存储模式、能力降级、安全与审计日志。']
  };

  const app = Vue.createApp({
    data() {
      return {
        route: 'dashboard',
        theme: 'light',
        token: localStorage.getItem(TOKEN_KEY) || '',
        tokenModal: false,
        globalKeyword: '',
        navGroups: [
          { title: '概览', items: [{ route: 'dashboard', icon: 'grid', label: '监控总览' }] },
          { title: '消息接入', items: [
            { route: 'servers', icon: 'server', label: '服务连接 (Server)' },
            { route: 'topics', icon: 'hash', label: 'Topic 管理' },
            { route: 'cluster', icon: 'network', label: '集群管理' }] },
          { title: '消息工厂', items: [
            { route: 'templates', icon: 'file-text', label: '消息模板' },
            { route: 'messaging', icon: 'send', label: '收发调试台' }] },
          { title: '运维', items: [
            { route: 'dlq', icon: 'inbox', label: '死信 DLQ' },
            { route: 'settings', icon: 'settings', label: '系统设置' }] }
        ],
        toasts: [], toastSeq: 0,
        ov: {}, dlqTotal: 0, dlqPreview: [],
        servers: [], srv: { keyword: '', protocol: '' },
        topics: [], topic: { serverId: '', keyword: '' },
        cl: { serverId: '', tab: 'ck', error: '', loading: false, overview: null, topics: [], groups: [],
              groupDetail: null, nodes: [], clients: [], clientFilter: '', mqttTopics: [], supported: [] },
        templates: [],
        tpl: { keyword: '', current: null, tab: 'tf', fieldsJson: '[]', matchPayload: '', matchResult: null,
               mtTyped: '', mtUnknown: null, mtViolations: [], versions: [] },
        ms: { serverId: '', filter: '', paused: false, mode: 'form', pubTopic: '', templateId: '',
              form: {}, formFields: [], rawJson: '', qos: 1, retain: false, key: '' },
        subs: [], feed: [],
        ws: { socket: null, open: false, wsId: 0, subscribed: null, watching: false },
        dlq: { topic: '', errCode: '', limit: 50, rows: [], total: 0, queryable: true },
        dlqSel: [], errCodes: [4001, 4002, 4003, 5001, 5002, 6001, 6002],
        auditRows: [], auditTotal: 0, auditPage: 0, auditQueryAble: false,
        fm: { kind: '', title: '', okLabel: '', f: {}, data: {} },
        danger: { open: false, title: '', desc: '', resource: '', input: '', action: '确认', onOk: null },
        jm: { open: false, title: '', text: '' }
      };
    },
    computed: {
      meta() { return META[this.route] || META.dashboard; },
      runningSubs() { return (this.ov.subscriptions || []).filter(s => s.state === 'RUNNING').length; },
      totalBuffers() { return Object.values(this.ov.publishBuffers || {}).reduce((a, b) => a + (Number(b) || 0), 0); },
      hasBuffers() { return Object.keys(this.ov.publishBuffers || {}).length > 0; },
      clServer() { return this.servers.find(s => s.id === this.cl.serverId) || null; },
      clAdminOps() {
        const s = this.clServer;
        if (s && s.mgmt && s.mgmt.allowAdminOps != null) return String(s.mgmt.allowAdminOps);
        return String(this.ov.allowAdminOps);
      },
      clTabs() {
        const s = this.clServer;
        if (!s || s.protocol === 'KAFKA') return [
          { key: 'ck', label: 'Kafka 概览' }, { key: 'ct', label: 'Topics' },
          { key: 'cg', label: '消费组 / Lag' }, { key: 'ca', label: 'ACL' }];
        return [
          { key: 'en', label: 'EMQX 概览 / 节点' }, { key: 'ec', label: '客户端' }, { key: 'er', label: 'Topics / Retained' }];
      }
    },
    methods: {
      /* ================= 基础 ================= */
      base() { return location.origin; },
      toast(type, msg) {
        const id = ++this.toastSeq;
        this.toasts.push({ id, type, msg });
        setTimeout(() => { const i = this.toasts.findIndex(t => t.id === id); if (i >= 0) this.toasts.splice(i, 1); }, 3500);
      },
      async run(fn, okMsg) {
        try {
          const r = await fn();
          if (okMsg) this.toast('success', okMsg);
          return r;
        } catch (e) {
          this.toast('error', e.message || String(e));
          if (e.http === 401) this.openToken();
          return undefined;
        }
      },
      openToken() {
        this.fm = { kind: 'token', title: '配置访问令牌', okLabel: '保存', f: { token: this.token } };
      },
      fmt,
      tryParse(s) { if (s == null) return null; try { return JSON.parse(s); } catch (e) { return s; } },
      jsonModal(title, obj) {
        this.jm = { open: true, title, text: typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2) };
      },
      exportJson(name, obj) {
        const blob = new Blob([JSON.stringify(obj, null, 2)], { type: 'application/json' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = name + '.json';
        a.click();
        URL.revokeObjectURL(a.href);
      },
      go(r) { this.route = r; },
      globalSearch() { this.srv.keyword = this.globalKeyword; this.go('servers'); this.loadServers(); },
      toggleTheme() {
        this.theme = this.theme === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', this.theme);
        try { localStorage.setItem('wb-theme', this.theme); } catch (e) {}
      },
      stateTag(st) {
        switch (st) {
          case 'CONNECTED': return { cls: 'tag--success', dot: 'dot--pulse', label: '已连接' };
          case 'DEGRADED': return { cls: 'tag--warning', dot: 'dot--deg', label: '降级' };
          case 'CONNECTING': return { cls: 'tag--warning', dot: 'dot--deg', label: '连接中' };
          case 'DOWN': return { cls: 'tag--danger', dot: 'dot--down', label: '离线' };
          default: return { cls: 'tag--info', dot: 'dot--off', label: '未连接' };
        }
      },
      statePct(st) {
        return { CONNECTED: 100, DEGRADED: 55, CONNECTING: 45, DOWN: 8 }[st] != null
          ? ({ CONNECTED: 100, DEGRADED: 55, CONNECTING: 45, DOWN: 8 }[st]) + '%' : '12%';
      },
      dirLabel(d) { return d === 'PUB' ? '发布' : d === 'SUB' ? '订阅' : '双向'; },
      isMqtt(id) { const s = this.servers.find(x => x.id === id); return s && s.protocol === 'MQTT'; },
      isKafka(id) { const s = this.servers.find(x => x.id === id); return !s || s.protocol === 'KAFKA'; },
      connAddr(s) {
        const c = s.conn || {};
        return c.bootstrapServers || c.serverUrls || c.host || (Object.keys(c).length ? JSON.stringify(c) : '—');
      },

      /* ================= 路由装载 ================= */
      async reload() { await this.loadRoute(this.route); },
      async loadRoute(r) {
        const fns = {
          dashboard: this.loadOverview, servers: this.loadServers, topics: this.loadTopics,
          cluster: this.loadCluster, templates: this.loadTemplates, messaging: this.loadMessaging,
          dlq: this.loadDlq, settings: this.loadSettings
        };
        if (fns[r]) await fns[r].call(this);
      },
      async init() {
        await this.run(async () => { this.ov = await req('GET', '/status/overview'); });
        this.auditQueryAble = (this.ov.runtimeCapabilities || []).includes('AUDIT_QUERY');
        this.dlq.queryable = (this.ov.runtimeCapabilities || []).includes('DLQ_QUERY');
        await Promise.all([this.loadServers(), this.loadTemplates(), this.loadDlqTotals()]);
        if (!this.ms.serverId && this.servers.length) this.ms.serverId = this.servers[0].id;
        if (!this.cl.serverId && this.servers.length) { this.cl.serverId = this.servers[0].id; this.cl.tab = this.clTabs[0].key; }
        if (this.topic.serverId === '' && this.servers.length) this.topic.serverId = this.servers[0].id;
        setInterval(async () => {
          if (this.route === 'dashboard') { await this.loadOverview(); await this.loadDlqTotals(); }
        }, 30000);
      },
      async loadOverview() {
        await this.run(async () => {
          this.ov = await req('GET', '/status/overview');
          this.auditQueryAble = (this.ov.runtimeCapabilities || []).includes('AUDIT_QUERY');
          this.dlq.queryable = (this.ov.runtimeCapabilities || []).includes('DLQ_QUERY');
        });
      },

      /* ================= Server ================= */
      async loadServers() {
        await this.run(async () => {
          const p = await req('GET', '/servers', { q: { protocol: this.srv.protocol, keyword: this.srv.keyword, size: 200 } });
          this.servers = p.items || [];
        });
      },
      async serverOp(s, op) {
        if (op === 'connect') this.toast('info', '正在连接 ' + s.name + '…');
        const r = await this.run(() => req('POST', '/servers/' + encodeURIComponent(s.id) + '/' + op), op === 'connect' ? '连接完成' : '已断开');
        if (r !== undefined) await this.loadServers();
      },
      openServer(s) {
        const c = (s && s.conn) || {};
        const rest = {};
        Object.entries(c).forEach(([k, v]) => {
          if (!['bootstrapServers', 'serverUrls', 'host', 'clientId', 'protocolVersion'].includes(k)) rest[k] = v;
        });
        this.fm = {
          kind: 'server', title: s ? '编辑 Server · ' + s.name : '注册 Server', okLabel: '保存',
          f: {
            id: s ? s.id : '', name: s ? s.name : '', protocol: s ? s.protocol : 'KAFKA',
            enabled: s ? s.enabled : true, connMode: s ? s.connMode : 'EAGER',
            addr: c.bootstrapServers || c.serverUrls || c.host || '', clientId: c.clientId || '',
            mqttVersion: c.protocolVersion || '5', baseUrl: (s && s.mgmt && s.mgmt.baseUrl) || '',
            adminOps: s && s.mgmt && s.mgmt.allowAdminOps != null ? String(s.mgmt.allowAdminOps) : '',
            connJson: Object.keys(rest).length ? JSON.stringify(rest) : '',
            username: '', password: '', mgmtApiKey: '', mgmtSecret: '',
            hasCred: s ? s.hasCredentials : false, createdAt: s ? s.createdAt : null,
            updatedAt: s ? s.updatedAt : null
          }
        };
      },
      delServer(s) {
        this.danger = {
          open: true, title: '删除 Server ' + s.name + '？', resource: s.name, input: '', action: '删除 Server',
          desc: '此操作不可逆，将断开连接并删除配置及其引用检查（被订阅/Topic 引用时需先清理，后端返回 IN_USE）。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/servers/' + encodeURIComponent(s.id), { q: { force: true } }), '已删除');
            await this.loadServers();
          }
        };
      },

      /* ================= Topic ================= */
      async loadTopics() {
        await this.run(async () => {
          const p = await req('GET', '/topics', { q: { serverId: this.topic.serverId, keyword: this.topic.keyword, size: 200 } });
          this.topics = p.items || [];
        });
      },
      openTopic(t) {
        this.fm = {
          kind: 'topic', title: t ? '编辑 Topic' : '登记 Topic', okLabel: '保存',
          f: {
            id: t ? t.id : '', namePattern: t ? t.namePattern : '', direction: t ? t.direction : 'BOTH',
            format: t ? t.format : 'json', qos: t ? t.qos : null, keyStrategy: t ? (t.keyStrategy || '') : '',
            templateId: t ? (t.templateId || '') : '', autoMatch: t ? t.autoMatch : true,
            owner: t ? (t.owner || '') : '', remark: t ? (t.remark || '') : '',
            serverId: this.topic.serverId
          }
        };
      },
      delTopic(t) {
        this.danger = {
          open: true, title: '删除 Topic ' + t.namePattern + '？', resource: t.namePattern, input: '', action: '删除 Topic',
          desc: '此操作不可逆。删除后该映射不再参与归类，已挂起的订阅可能受影响。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/topics/' + encodeURIComponent(t.id), { q: { force: true } }), '已删除');
            await this.loadTopics();
          }
        };
      },
      async browseTopics() {
        if (!this.topic.serverId) { this.toast('error', '请先选择 Server'); return; }
        await this.run(async () => {
          const d = await req('GET', '/topics/browse', { q: { serverId: this.topic.serverId } });
          this.fm = { kind: 'browse', title: 'Broker Topic 浏览', okLabel: '关闭', f: {}, data: d };
        });
      },

      /* ================= 集群管理 ================= */
      async loadCluster() {
        if (!this.cl.serverId && this.servers.length) this.cl.serverId = this.servers[0].id;
        const t = this.clTabs.map(x => x.key);
        if (!t.includes(this.cl.tab)) this.cl.tab = t[0];
        await this.loadClusterTab();
      },
      async loadClusterTab() {
        const id = encodeURIComponent(this.cl.serverId);
        this.cl.error = ''; this.cl.loading = true;
        const K = '/mgmt/kafka/' + id, E = '/mgmt/emqx/' + id;
        try {
          switch (this.cl.tab) {
            case 'ck': this.cl.overview = await req('GET', K + '/overview'); break;
            case 'ct': this.cl.topics = await req('GET', K + '/topics'); break;
            case 'cg': this.cl.groups = await req('GET', K + '/groups'); this.cl.groupDetail = null; break;
            case 'ca': break;
            case 'en':
              this.cl.overview = await req('GET', E + '/overview');
              try { this.cl.nodes = await req('GET', E + '/nodes'); } catch (e) { this.cl.nodes = []; }
              try { this.cl.supported = (await req('GET', E + '/operations')).supported || []; } catch (e) { this.cl.supported = []; }
              break;
            case 'ec': this.cl.clients = await req('GET', E + '/clients', { q: { filter: this.cl.clientFilter } }); break;
            case 'er': this.cl.mqttTopics = await req('GET', E + '/topics'); break;
          }
        } catch (e) {
          this.cl.error = e.message || String(e);
        } finally { this.cl.loading = false; }
      },
      overviewRows(o) {
        const out = {};
        Object.entries(o).forEach(([k, v]) => {
          out[k] = Array.isArray(v) ? v.map(x => typeof x === 'object' ? JSON.stringify(x) : x).join(', ') : v;
        });
        return out;
      },
      openCreateTopic() {
        this.fm = { kind: 'createTopic', title: '新建 Kafka Topic', okLabel: '创建',
          f: { name: '', partitions: 3, replicationFactor: 1, configJson: '' } };
      },
      openAddPartitions(t) {
        this.fm = { kind: 'addPartitions', title: '扩容分区 · ' + t.topic, okLabel: '扩容',
          f: { topic: t.topic, current: t.partitions, total: t.partitions } };
      },
      delKafkaTopic(t) {
        this.danger = {
          open: true, title: '删除 Kafka Topic ' + t.topic + '？', resource: t.topic, input: '', action: '删除 Topic',
          desc: '此操作不可逆，' + t.partitions + ' 个分区上的消息将全部丢失。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId)
              + '/topics/' + encodeURIComponent(t.topic), { q: { confirm: t.topic } }), '已提交删除');
            await this.loadClusterTab();
          }
        };
      },
      async kafkaTopicDetail(topic) {
        await this.run(async () => {
          const d = await req('GET', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId) + '/topics/' + encodeURIComponent(topic));
          const s = await req('GET', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId) + '/topics/' + encodeURIComponent(topic) + '/stats');
          this.jsonModal('Topic ' + topic, { detail: d, stats: s });
        });
      },
      async kafkaGroupDetail(g) {
        await this.run(async () => {
          this.cl.groupDetail = await req('GET', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId) + '/groups/' + encodeURIComponent(g.groupId));
        });
      },
      openResetOffsets(g) {
        this.fm = { kind: 'resetOffsets', title: '重置 offset · ' + g.groupId, okLabel: '下一步（确认）',
          f: { gid: g.groupId, topic: '', strategy: 'to-latest', timestamp: null } };
      },
      clearRetained(t) {
        this.danger = {
          open: true, title: '清除 Retained · ' + t.topic + '？', resource: t.topic, input: '', action: '清除消息',
          desc: '该 topic 的 retained 消息将被清除，后到的订阅者不会再收到缓存报文。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/mgmt/emqx/' + encodeURIComponent(this.cl.serverId) + '/retained',
              { body: { topic: t.topic, confirm: t.topic } }), '已清除');
            await this.loadClusterTab();
          }
        };
      },

      /* ================= 模板 ================= */
      async loadTemplates() {
        await this.run(async () => {
          const p = await req('GET', '/templates', { q: { keyword: this.tpl.keyword, size: 200 } });
          this.templates = p.items || [];
          if (!this.tpl.current && this.templates.length) await this.selectTemplate(this.templates[0]);
        });
      },
      async selectTemplate(t) {
        await this.run(async () => {
          const d = await req('GET', '/templates/' + encodeURIComponent(t.id));
          this.tpl.current = d;
          this.tpl.fieldsJson = JSON.stringify(d.fields || [], null, 2);
          this.tpl.matchResult = null; this.tpl.versions = [];
        });
      },
      flatFields(fields) {
        const out = [];
        const walk = (arr, level, prefix) => (arr || []).forEach(f => {
          out.push({
            name: f.name, type: f.type, required: f.required, comment: f.comment, example: f.example, level,
            path: prefix + '.' + f.name,
            enumText: (f.enumValues && f.enumValues.length) ? f.enumValues.join('|') : null
          });
          if (f.children) walk(f.children, Math.min(level + 1, 2), prefix + '.' + f.name);
        });
        walk(fields, 0, '');
        return out;
      },
      typeTag(t) {
        return { string: 'tag--primary', number: 'tag--accent', integer: 'tag--accent',
                 boolean: 'tag--warning', object: 'tag--info', array: 'tag--info' }[t] || 'tag--info';
      },
      async saveTpl() {
        const cur = this.tpl.current;
        let fields;
        try { fields = JSON.parse(this.tpl.fieldsJson); } catch (e) { this.toast('error', '字段 JSON 非法: ' + e.message); return; }
        const body = { id: cur.id, name: cur.name, description: cur.description, fields };
        const doPut = async (force) => {
          const saved = await req('PUT', '/templates/' + encodeURIComponent(cur.id) + (force ? '?force=true' : ''), { body });
          this.toast('success', '已保存 v' + saved.version);
          await this.selectTemplate(saved); await this.loadTemplates();
        };
        try { await doPut(false); } catch (e) {
          if (e.code === 6002) {
            this.danger = {
              open: true, title: '不兼容变更（6002）', resource: cur.id, input: '', action: '强制保存新版本',
              desc: 'VersionGuard 拦截：' + e.message + ' 若确认下游可接受，输入模板 ID 强制升级版本。',
              onOk: () => this.run(() => doPut(true))
            };
          } else this.toast('error', e.message);
        }
      },
      async toggleTplStatus() {
        const cur = this.tpl.current;
        const next = cur.status === 'ENABLED' ? 'DISABLED' : 'ENABLED';
        await this.run(() => req('POST', '/templates/' + encodeURIComponent(cur.id) + '/status', { body: { status: next } }), '状态 → ' + next);
        await this.selectTemplate({ id: cur.id }); await this.loadTemplates();
      },
      delTemplate() {
        const cur = this.tpl.current;
        this.danger = {
          open: true, title: '删除模板 ' + cur.id + '？', resource: cur.id, input: '', action: '删除模板',
          desc: '此操作不可逆。被 Topic/订阅引用时后端将拒绝（IN_USE），可勾选强制。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/templates/' + encodeURIComponent(cur.id), { q: { force: true } }), '已删除');
            this.tpl.current = null; await this.loadTemplates();
          }
        };
      },
      openNewTpl() {
        this.fm = { kind: 'newTpl', title: '新建模板', okLabel: '创建',
          f: { id: '', name: '', description: '',
               fieldsJson: JSON.stringify([{ name: 'field1', type: 'string', required: true, comment: '' }], null, 2) } };
      },
      openInfer() {
        this.fm = { kind: 'infer', title: '样例反推模板', okLabel: '反推',
          f: { samples: '', name: '' } };
      },
      async runMatchTest() {
        if (!this.tpl.matchPayload) { this.toast('error', '请输入样例 payload'); return; }
        await this.run(async () => {
          const d = await req('POST', '/templates/match-test', {
            body: { payload: this.tpl.matchPayload, templateIds: this.tpl.current ? [this.tpl.current.id] : null } });
          this.tpl.matchResult = d.match || {};
          this.tpl.mtTyped = d.typedJson || '';
          this.tpl.mtUnknown = d.unknownFields || null;
          this.tpl.mtViolations = d.violations || [];
        });
      },
      async loadVersions() {
        if (!this.tpl.current) return;
        await this.run(async () => {
          this.tpl.versions = await req('GET', '/templates/' + encodeURIComponent(this.tpl.current.id) + '/versions');
        });
      },

      /* ================= 调试台 / WS ================= */
      async loadMessaging() {
        await this.loadSubs();
        this.ensureWs();
      },
      onMsServer() { this.loadSubs(); },
      ensureWs() {
        if (this.ws.socket && (this.ws.socket.readyState === 0 || this.ws.socket.readyState === 1)) return;
        const proto = location.protocol === 'https:' ? 'wss' : 'ws';
        const sock = new WebSocket(proto + '://' + location.host + '/ws');
        this.ws.socket = sock;
        sock.onopen = () => {
          this.ws.open = true;
          if (this.ws.subscribed) this.wsSubscribe();
          if (!this.ws.watching) { this.wsSend({ action: 'watch_status' }); this.ws.watching = true; }
        };
        sock.onclose = () => {
          this.ws.open = false; this.ws.socket = null;
          if (this.route === 'messaging') setTimeout(() => this.ensureWs(), 4000);
        };
        sock.onerror = () => { this.ws.open = false; };
        sock.onmessage = (ev) => {
          let f; try { f = JSON.parse(ev.data); } catch (e) { return; }
          if (f.type === 'msg_frame') {
            if (this.ms.paused) return;
            this.feed.unshift(Object.assign({ open: false }, f));
            if (this.feed.length > 200) this.feed.length = 200;
          } else if (f.type === 'result') {
            if (f.error) this.toast('error', 'WS 结果: ' + f.error.message);
            else this.toast('success', 'WS 结果: ' + JSON.stringify(f.data));
          } else if (f.type === 'event') {
            // 状态事件仅提示关键项
            if (f.event && String(f.event).indexOf('CONNECT') >= 0) this.toast('info', '事件: ' + f.event + ' · ' + (f.subject || ''));
          }
        };
      },
      wsSend(o) {
        if (!this.ws.socket || this.ws.socket.readyState !== 1) { this.toast('error', 'WS 未连接'); return false; }
        o.wsId = ++this.ws.wsId;
        this.ws.socket.send(JSON.stringify(o));
        return true;
      },
      wsSubscribe() {
        if (!this.ms.serverId) { this.toast('error', '请选择 Server'); return; }
        this.ws.subscribed = { serverId: this.ms.serverId, topic: this.ms.filter || '' };
        if (this.wsSend({ action: 'subscribe', serverId: this.ms.serverId, topic: this.ms.filter || '' }))
          this.toast('success', '已订阅调试流（不计入正式订阅）');
      },
      wsUnsubscribe() {
        if (!this.ws.subscribed) return;
        this.wsSend({ action: 'unsubscribe', serverId: this.ws.subscribed.serverId });
        this.ws.subscribed = null;
        this.toast('info', '已取消调试订阅');
      },
      feedTag(f) {
        if (f.parseError) return { cls: 'tag--danger', label: '<span class="dot dot--down"></span>解析失败' };
        switch (f.matchType) {
          case 'EXPLICIT': return { cls: 'tag--primary', label: 'EXPLICIT' };
          case 'BOUND': return { cls: 'tag--success', label: '<span class="dot dot--on"></span>BOUND' };
          case 'SIMILARITY': return { cls: 'tag--warning', label: 'SIMILARITY ' + (f.matchScore || 0).toFixed(2) };
          default: return { cls: 'tag--info', label: '未归类' };
        }
      },
      async buildForm() {
        this.ms.formFields = []; this.ms.form = {};
        if (!this.ms.templateId) return;
        await this.run(async () => {
          const t = await req('GET', '/templates/' + encodeURIComponent(this.ms.templateId));
          this.ms.formFields = (t.fields || []).slice();
          const init = {};
          this.ms.formFields.forEach(f => {
            if (f.enumValues && f.enumValues.length) init[f.name] = f.enumValues[0];
            else if (f.type === 'object' || f.type === 'array') init[f.name] = f.example != null ? JSON.stringify(f.example) : '';
            else init[f.name] = f.example != null ? String(f.example) : '';
          });
          this.ms.form = init;
        });
      },
      collectPayload() {
        if (this.ms.mode === 'json') {
          try { return JSON.parse(this.ms.rawJson); } catch (e) { throw new Error('payload JSON 非法: ' + e.message); }
        }
        const p = {};
        this.ms.formFields.forEach(f => {
          const v = this.ms.form[f.name];
          if (v === '' || v == null) { if (f.required) throw new Error('必填字段缺失: ' + f.name); return; }
          if (f.type === 'number' || f.type === 'integer') {
            const n = Number(v);
            if (isNaN(n)) throw new Error(f.name + ' 需为数字');
            p[f.name] = n;
          } else if (f.type === 'boolean') {
            p[f.name] = String(v) === 'true';
          } else if (f.type === 'object' || f.type === 'array') {
            try { p[f.name] = JSON.parse(v); } catch (e) { throw new Error(f.name + ' 需为合法 JSON'); }
          } else p[f.name] = v;
        });
        return p;
      },
      async doPublish() {
        let payload;
        try { payload = this.collectPayload(); } catch (e) { this.toast('error', e.message); return; }
        if (!this.ms.serverId || !this.ms.pubTopic) { this.toast('error', 'Server 与 Topic 必填'); return; }
        const body = { serverId: this.ms.serverId, topic: this.ms.pubTopic, payload };
        if (this.ms.templateId) { body.templateId = this.ms.templateId; }
        if (this.isMqtt(this.ms.serverId)) { body.qos = this.ms.qos; body.retain = this.ms.retain; }
        if (this.ms.key) body.key = this.ms.key;
        const r = await this.run(() => req('POST', '/publish', { body }));
        if (r !== undefined) this.jsonModal('发布结果', r);
      },
      async validateOnly() {
        let payload;
        try { payload = this.collectPayload(); } catch (e) { this.toast('error', e.message); return; }
        await this.run(async () => {
          const d = await req('POST', '/templates/match-test', {
            body: { payload: JSON.stringify(payload), templateIds: this.ms.templateId ? [this.ms.templateId] : null } });
          this.jsonModal('校验 / 匹配结果', d);
        });
      },

      /* ================= 正式订阅 ================= */
      async loadSubs() {
        await this.run(async () => {
          this.subs = await req('GET', '/subscriptions', { q: { serverId: this.ms.serverId } });
        });
      },
      openSub(sb) {
        this.fm = {
          kind: 'sub', title: sb ? '编辑订阅 · ' + sb.id : '新建订阅', okLabel: '保存',
          f: {
            id: sb ? sb.id : '', serverId: this.ms.serverId, topic: sb ? sb.topic : this.ms.pubTopic,
            group: sb ? (sb.group || '') : '', qos: sb && sb.qos != null ? sb.qos : 1,
            offsetStrategy: sb ? (sb.offsetStrategy || 'latest') : 'latest',
            templateIdsText: sb ? (sb.templateIds || []).join(',') : '',
            autoMatch: sb ? sb.autoMatch : true, dlqEnabled: sb ? sb.dlqEnabled : true,
            persistent: sb ? sb.persistent : false, start: sb ? false : true
          }
        };
      },
      delSub(sb) {
        this.danger = {
          open: true, title: '删除订阅 ' + sb.id + '？', resource: sb.id, input: '', action: '删除订阅',
          desc: '运行中的订阅将被停止并取消 Broker 侧订阅，此操作不可逆。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/subscriptions/' + encodeURIComponent(sb.id)), '已删除');
            await this.loadSubs();
          }
        };
      },
      async subOp(sb, op) {
        await this.run(() => req('POST', '/subscriptions/' + encodeURIComponent(sb.id) + '/' + op), op + ' 完成');
        await this.loadSubs();
      },

      /* ================= DLQ ================= */
      async loadDlqTotals() {
        await this.run(async () => {
          const p = await req('GET', '/dlq/tail', { q: { limit: 5 } });
          this.dlqTotal = p.total || 0;
          this.dlqPreview = (p.items || []).slice(0, 3);
        });
      },
      async loadDlq() {
        await this.run(async () => {
          let p;
          if (this.dlq.queryable) {
            p = await req('GET', '/dlq', { q: { topic: this.dlq.topic, errCode: this.dlq.errCode, size: this.dlq.limit } });
          } else {
            p = await req('GET', '/dlq/tail', { q: { limit: this.dlq.limit } });
          }
          this.dlq.rows = p.items || []; this.dlq.total = p.total || 0; this.dlqSel = [];
          this.dlqTotal = p.total || 0;
        });
      },
      toggleAllDlq(e) {
        this.dlqSel = e.target.checked ? this.dlq.rows.map(r => r.id) : [];
      },
      async replay(ids) {
        if (!ids.length) return;
        const r = await this.run(() => req('POST', '/dlq/replay', { body: { ids } }));
        if (r !== undefined) {
          this.toast('success', '重放 ' + r.replayed + '/' + r.requested + (r.failed.length ? '，失败 ' + r.failed.length : ''));
          await this.loadDlq();
        }
      },
      drop(ids) {
        this.danger = {
          open: true, title: '丢弃 ' + ids.length + ' 条死信？', resource: String(ids.length), input: '', action: '丢弃',
          desc: '选中的死信记录将被永久删除且不可重放，输入条数确认。',
          onOk: async () => {
            await this.run(() => req('DELETE', '/dlq', { body: { ids } }), '已丢弃');
            await this.loadDlq();
          }
        };
      },

      /* ================= 设置 / 审计 ================= */
      async loadSettings() { await this.loadOverview(); if (this.auditQueryAble) await this.loadAudit(); },
      async loadAudit() {
        await this.run(async () => {
          const d = await req('GET', '/status/audit', { q: { page: this.auditPage, size: 10 } });
          this.auditRows = d.items || []; this.auditTotal = d.total || 0;
        });
      },

      /* ================= 模态提交 ================= */
      async fmSubmit() {
        const k = this.fm.kind, f = this.fm.f;
        if (k === 'browse' || k === 'token' && !f.token) { if (k === 'token') { localStorage.removeItem(TOKEN_KEY); this.token = ''; } this.fm.kind = ''; return; }
        switch (k) {
          case 'server': {
            if (!f.name) { this.toast('error', '名称必填'); return; }
            if (!f.addr) { this.toast('error', '连接地址必填'); return; }
            const conn = {};
            conn[f.protocol === 'KAFKA' ? 'bootstrapServers' : 'serverUrls'] = f.addr;
            if (f.clientId) conn.clientId = f.clientId;
            if (f.protocol === 'MQTT') conn.protocolVersion = f.mqttVersion;
            if (f.connJson) { try { Object.assign(conn, JSON.parse(f.connJson)); } catch (e) { this.toast('error', '高级 conn JSON 非法'); return; } }
            const credentials = {};
            if (f.username) credentials.username = f.username;
            if (f.password) credentials.password = f.password;
            if (f.mgmtApiKey) credentials.mgmtApiKey = f.mgmtApiKey;
            if (f.mgmtSecret) credentials.mgmtSecret = f.mgmtSecret;
            const mgmt = {};
            if (f.baseUrl) mgmt.baseUrl = f.baseUrl;
            if (f.adminOps !== '') mgmt.allowAdminOps = f.adminOps === 'true';
            const body = { name: f.name, protocol: f.protocol, enabled: !!f.enabled, connMode: f.connMode,
                           conn, credentials, mgmt, createdAt: f.createdAt };
            if (f.id) body.id = f.id;
            const saved = await this.run(() => req('POST', '/servers', { body }), f.id ? '已更新' : '已注册');
            if (saved) { this.fm.kind = ''; await this.loadServers(); }
            return;
          }
          case 'topic': {
            if (!f.serverId) { this.toast('error', '请先在上方选择 Server'); return; }
            if (!f.namePattern) { this.toast('error', 'Topic 名必填'); return; }
            const body = Object.assign({}, f); delete body.serverId;
            body.serverId = f.serverId;
            if (!body.id) delete body.id;
            const saved = await this.run(() => req('POST', '/topics', { body }), '已保存 Topic');
            if (saved) { this.fm.kind = ''; await this.loadTopics(); }
            return;
          }
          case 'sub': {
            const subscription = {
              serverId: f.serverId, topic: f.topic, group: f.group || null,
              qos: f.qos, autoMatch: f.autoMatch, dlqEnabled: f.dlqEnabled,
              persistent: f.persistent, offsetStrategy: f.offsetStrategy,
              templateIds: f.templateIdsText ? f.templateIdsText.split(/[,，\s]+/).filter(Boolean) : []
            };
            if (f.id) subscription.id = f.id;
            if (this.isKafka(f.serverId)) delete subscription.qos; else subscription.offsetStrategy = null;
            const saved = await this.run(() => req('POST', '/subscriptions', { body: { subscription, start: f.start } }), '订阅已保存');
            if (saved) { this.fm.kind = ''; await this.loadSubs(); }
            return;
          }
          case 'createTopic': {
            const body = { name: f.name, partitions: f.partitions, replicationFactor: f.replicationFactor };
            if (f.configJson) { try { body.config = JSON.parse(f.configJson); } catch (e) { this.toast('error', 'config JSON 非法'); return; } }
            const r = await this.run(() => req('POST', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId) + '/topics/_create', { body }), 'Topic 已创建');
            if (r) { this.fm.kind = ''; await this.loadClusterTab(); }
            return;
          }
          case 'addPartitions': {
            const r = await this.run(() => req('POST', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId)
              + '/topics/' + encodeURIComponent(f.topic) + '/_add-partitions', { body: { totalPartitions: f.total } }), '已扩容');
            if (r) { this.fm.kind = ''; await this.loadClusterTab(); }
            return;
          }
          case 'resetOffsets': {
            if (!f.topic || !f.strategy) { this.toast('error', 'topic/strategy 必填'); return; }
            this.fm.kind = '';
            this.danger = {
              open: true, title: '重置消费组 ' + f.gid + ' 的 offset？', resource: f.gid, input: '', action: '重置 offset',
              desc: '将对 ' + f.topic + ' 执行 ' + f.strategy + (f.timestamp ? ' @' + f.timestamp : '') + '，消费者随后按新位点重放，不可逆。',
              onOk: () => this.run(() => req('POST', '/mgmt/kafka/' + encodeURIComponent(this.cl.serverId)
                + '/groups/' + encodeURIComponent(f.gid) + '/_reset-offsets',
                { q: { confirm: f.gid }, body: { topic: f.topic, strategy: f.strategy, timestamp: f.timestamp } }), 'offset 已重置')
            };
            return;
          }
          case 'infer': {
            const samples = (f.samples || '').split(/\n\s*\n|\n(?=\s*[{\[])/).map(s => s.trim()).filter(Boolean);
            if (!samples.length) { this.toast('error', '请粘贴至少一条样例 JSON'); return; }
            const body = { samples, save: !!f.name };
            if (f.name) body.name = f.name;
            const d = await this.run(() => req('POST', '/templates/infer', { body }));
            if (!d) return;
            this.fm.kind = '';
            if (d.template) {
              await this.loadTemplates();
              await this.selectTemplate(d.template);
              this.tpl.current = d.template;
              this.tpl.fieldsJson = JSON.stringify(d.fields || d.template.fields || [], null, 2);
              this.toast('success', '已保存草稿 ' + d.template.id);
            } else {
              this.tpl.fieldsJson = JSON.stringify(d.fields || [], null, 2);
              this.tpl.tab = 'tf';
              this.toast('success', '反推完成，已填入当前模板字段编辑区');
            }
            return;
          }
          case 'newTpl': {
            let fields; try { fields = JSON.parse(f.fieldsJson); } catch (e) { this.toast('error', '字段 JSON 非法'); return; }
            if (!f.id || !f.name) { this.toast('error', 'ID 与名称必填'); return; }
            const body = { id: f.id, name: f.name, description: f.description, status: 'DRAFT', fields, source: 'MANUAL' };
            const saved = await this.run(() => req('POST', '/templates', { body }), '模板已创建');
            if (saved) {
              this.fm.kind = ''; await this.loadTemplates();
              await this.selectTemplate(saved); this.tpl.current = saved;
              this.tpl.fieldsJson = JSON.stringify(saved.fields || [], null, 2);
            }
            return;
          }
          case 'token': {
            localStorage.setItem(TOKEN_KEY, f.token || '');
            this.token = f.token || '';
            this.tokenModal = false; this.fm.kind = '';
            this.toast('success', 'Token 已保存到本地');
            await this.reload();
            return;
          }
        }
      },
      async dangerConfirm() {
        if (this.danger.input !== this.danger.resource) return;
        const fn = this.danger.onOk;
        this.danger = { ...this.danger, open: false };
        if (fn) await fn();
      }
    },
    created() {
      const r = (location.hash || '').replace(/^#\/?/, '');
      this.route = META[r] ? r : 'dashboard';
      this.init();
      window.addEventListener('hashchange', () => {
        const nr = (location.hash || '').replace(/^#\/?/, '');
        if (META[nr]) this.route = nr;
      });
    },
    watch: {
      route(r) {
        if (location.hash !== '#/' + r) location.hash = '#/' + r;
        this.loadRoute(r);
        if (r === 'messaging') this.ensureWs();
      }
    },
    mounted() {
      let saved = null; try { saved = localStorage.getItem('wb-theme'); } catch (e) {}
      if (saved) { this.theme = saved; document.documentElement.setAttribute('data-theme', saved); }
    }
  });

  app.config.errorHandler = (err) => { console.error(err); };
  app.mount('#app');
})();
