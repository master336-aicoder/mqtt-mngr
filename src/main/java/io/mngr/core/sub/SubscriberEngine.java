package io.mngr.core.sub;

import io.mngr.codec.json.JsonCodec;
import io.mngr.config.MngrProperties;
import io.mngr.core.enums.*;
import io.mngr.core.evt.EventBus;
import io.mngr.core.exception.MngrException;
import io.mngr.core.metrics.MngrMetrics;
import io.mngr.core.model.*;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.spi.ProtocolAdapter;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.core.template.TemplateMatcher;
import io.mngr.core.topic.TopicManager;
import io.mngr.core.topic.WildcardMatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/** 消费分发管线（§7.2）：sink → 背压队列 → worker → Codec → 模板匹配 → fanout/DLQ。 */
@Slf4j
public class SubscriberEngine {

    private static class Active {
        SubscriptionDef def;
        String handle;
        BackpressureQueue queue;
        Thread worker;
        volatile boolean running = true;
    }

    private final Map<String, Active> actives = new ConcurrentHashMap<>();
    private final List<Consumer<MessageContext>> debugSinks = new java.util.concurrent.CopyOnWriteArrayList<>();

    private final ConfigRepository repo;
    private final ServerManager serverManager;
    private final TopicManager topicManager;
    private final JsonCodec codec;
    private final TemplateMatcher matcher;
    private final MngrMetrics metrics;
    private final RuntimeRepository runtime;
    private final MngrProperties props;
    private final EventBus bus;

    public SubscriberEngine(ConfigRepository repo, ServerManager serverManager, TopicManager topicManager,
                            JsonCodec codec, TemplateMatcher matcher, MngrMetrics metrics,
                            RuntimeRepository runtime, MngrProperties props, EventBus bus) {
        this.repo = repo;
        this.serverManager = serverManager;
        this.topicManager = topicManager;
        this.codec = codec;
        this.matcher = matcher;
        this.metrics = metrics;
        this.runtime = runtime;
        this.props = props;
        this.bus = bus;
    }

    public void registerDebugSink(Consumer<MessageContext> sink) { debugSinks.add(sink); }

    public boolean isRunning(String subId) { return actives.containsKey(subId); }

    public void start(String subId) {
        if (actives.containsKey(subId)) return; // FR-2.3.3 幂等
        SubscriptionDef def = repo.findSubscription(subId)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND, "订阅不存在: " + subId));
        ServerConfig cfg = serverManager.getConfig(def.getServerId());
        serverManager.ensureConnected(def.getServerId());
        String topic = topicManager.resolveSubscribeTopic(def.getServerId(), def.getTopicId(), def.getTopic());
        def.setTopic(topic);

        Active a = new Active();
        a.def = def;
        a.queue = new BackpressureQueue(props.getDispatch().getQueueCapacity());
        ProtocolAdapter adapter = serverManager.adapter(cfg.getProtocol());
        a.handle = adapter.startSubscription(def, raw -> {
            metrics.consume(cfg.getProtocol().name(), def.getServerId());
            a.queue.offer(raw);
        });
        a.worker = new Thread(() -> workerLoop(subId, a), "mngr-dispatch-" + subId + "-0");
        a.worker.setDaemon(true);
        a.worker.start();
        actives.put(subId, a);

        def.setState("RUNNING");
        repo.saveSubscription(def);
        bus.fire("subscription", def.getServerId(), Map.of("subId", subId, "action", "start", "topic", topic));
    }

    public void stop(String subId) {
        Active a = actives.remove(subId);
        if (a == null) return;
        a.running = false;
        a.worker.interrupt();
        try {
            ServerConfig cfg = serverManager.getConfig(a.def.getServerId());
            serverManager.adapter(cfg.getProtocol()).stopSubscription(cfg.getId(), a.handle);
        } catch (Exception e) {
            log.debug("stopSubscription server-side cleanup skipped: {}", e.toString());
        }
        repo.findSubscription(subId).ifPresent(d -> {
            d.setState("STOPPED");
            repo.saveSubscription(d);
        });
        bus.fire("subscription", a.def.getServerId(), Map.of("subId", subId, "action", "stop"));
    }

    public void pause(String subId) {
        Active a = actives.get(subId);
        if (a == null) throw new MngrException(ErrorCode.NOT_FOUND, "订阅未运行: " + subId);
        a.queue.setPaused(true);
        repo.findSubscription(subId).ifPresent(d -> {
            d.setState("PAUSED");
            repo.saveSubscription(d);
        });
        bus.fire("subscription", a.def.getServerId(), Map.of("subId", subId, "action", "pause"));
    }

    public void resume(String subId) {
        Active a = actives.get(subId);
        if (a == null) throw new MngrException(ErrorCode.NOT_FOUND, "订阅未运行: " + subId);
        a.queue.setPaused(false);
        repo.findSubscription(subId).ifPresent(d -> {
            d.setState("RUNNING");
            repo.saveSubscription(d);
        });
        bus.fire("subscription", a.def.getServerId(), Map.of("subId", subId, "action", "resume"));
    }

    public List<Map<String, Object>> activeStatus() {
        List<Map<String, Object>> out = new ArrayList<>();
        actives.forEach((id, a) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("subId", id);
            m.put("serverId", a.def.getServerId());
            m.put("topic", a.def.getTopic());
            m.put("state", a.queue.isPaused() ? "PAUSED" : "RUNNING");
            m.put("queueSize", a.queue.size());
            m.put("dropped", a.queue.droppedCount());
            out.add(m);
        });
        out.sort(Comparator.comparing(m -> String.valueOf(m.get("subId"))));
        return out;
    }

    /** 重连钩子：恢复该 server 上 persistent 且 RUNNING/PAUSED 的订阅。 */
    public void restoreFor(String serverId) {
        for (SubscriptionDef d : repo.listSubscriptions(serverId)) {
            if (!d.isPersistent()) continue;
            if ("RUNNING".equals(d.getState()) || "PAUSED".equals(d.getState())) {
                try {
                    if (serverManager.status(serverId).getState() != ServerState.CONNECTED) continue;
                    stop(d.getId());
                    start(d.getId());
                    if ("PAUSED".equals(d.getState())) pause(d.getId());
                    log.info("restored subscription {} after reconnect", d.getId());
                } catch (Exception e) {
                    log.warn("restore subscription {} failed: {}", d.getId(), e.toString());
                }
            }
        }
    }

    // ---------------- pipeline ----------------

    private void workerLoop(String subId, Active a) {
        while (a.running) {
            try {
                if (a.queue.isPaused()) {
                    Thread.sleep(200);
                    continue;
                }
                BackpressureQueue.Enqueued e = a.queue.poll(Duration.ofMillis(300));
                if (e == null) continue;
                process(a.def, e.getRaw());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception ex) {
                log.warn("dispatch worker sub={} error", subId, ex);
            }
        }
    }

    void process(SubscriptionDef def, RawIncoming raw) {
        MessageContext.MessageContextBuilder ctx = MessageContext.builder()
                .serverId(def.getServerId()).subscriptionId(def.getId()).topic(raw.getTopic())
                .headers(raw.getHeaders()).protocolMeta(raw.getProtocolMeta()).receiveTs(raw.getReceiveTs())
                .rawBytes(raw.getPayload()).raw(null);
        String parseError = null;
        com.fasterxml.jackson.databind.JsonNode decoded = null;
        try {
            decoded = codec.decode(raw.getPayload());
        } catch (MngrException me) {
            parseError = me.getMessage();
            metrics.decodeFail();
        }
        final com.fasterxml.jackson.databind.JsonNode node = decoded;
        ctx.parseError(parseError);
        if (node != null) ctx.raw(node);

        List<String> boundIds = resolveBound(def, raw.getTopic());
        boolean autoMatch = def.isAutoMatch();
        TemplateMatcher.MatchResult mr = TemplateMatcher.MatchResult.none();
        if (node != null) {
            String explicit = raw.getHeaders() == null ? null : raw.getHeaders().get("x-template");
            // boundIds==null → 扫描全部启用模板（autoMatch）；空列表 → 不扫描（仅 EXPLICIT）
            mr = matcher.match(explicit,
                    autoMatch ? (boundIds.isEmpty() ? null : boundIds) : boundIds, node);
        }
        ctx.templateId(mr.getTemplateId()).templateVersion(mr.getVersion())
                .matchType(mr.getType()).matchScore(mr.getScore());

        if (mr.getTemplateId() != null && node != null) {
            repo.findTemplate(mr.getTemplateId(), mr.getVersion() == null ? 0 : mr.getVersion())
                    .ifPresent(t -> {
                        TypedPayload tp = codec.apply(node, t.getFields());
                        ctx.typed(tp);
                        if (!tp.getViolations().isEmpty()) {
                            deliverOrFail(def, raw, ctx, tp.getViolations(), ErrorCode.TEMPLATE_MISMATCH.code());
                            return;
                        }
                    });
            metrics.templateMatched(mr.getType().name());
        } else if (node != null) {
            metrics.unclassified();
        }

        MessageContext c = ctx.build();
        if (parseError != null) {
            toDlq(def, raw, ErrorCode.DECODE_FAILED.code(), parseError);
            for (Consumer<MessageContext> s : debugSinks) safeAccept(s, c);
            return;
        }
        for (Consumer<MessageContext> s : debugSinks) safeAccept(s, c);
    }

    private void deliverOrFail(SubscriptionDef def, RawIncoming raw, MessageContext.MessageContextBuilder ctx,
                               List<String> violations, int code) {
        ctx.parseError(String.join("; ", violations));
        toDlqRaw(def, raw, code, String.join("; ", violations));
    }

    private List<String> resolveBound(SubscriptionDef def, String topic) {
        if (def.getTemplateIds() != null && !def.getTemplateIds().isEmpty()) return def.getTemplateIds();
        // TopicConfig 默认模板（含通配模式命中）
        for (TopicConfig t : repo.listTopics(def.getServerId(), null, 0, 10000)) {
            if (t.getTemplateId() != null && !t.getTemplateId().isBlank()
                    && WildcardMatcher.matches(t.getNamePattern(), topic)) {
                return List.of(t.getTemplateId());
            }
        }
        return List.of();
    }

    private void safeAccept(Consumer<MessageContext> s, MessageContext c) {
        try { s.accept(c); } catch (Exception ignored) { }
    }

    private void toDlq(SubscriptionDef def, RawIncoming raw, int code, String msg) {
        toDlqRaw(def, raw, code, msg);
    }

    private void toDlqRaw(SubscriptionDef def, RawIncoming raw, int code, String msg) {
        if (!def.isDlqEnabled()) return;
        runtime.appendDlq(DlqRecord.builder()
                .serverId(def.getServerId())
                .topic(raw == null ? def.getTopic() : raw.getTopic())
                .subscriptionId(def.getId())
                .headers(raw == null ? Map.of() : raw.getHeaders())
                .payload(raw == null ? new byte[0] : raw.getPayload())
                .errCode(code).errMsg(msg).origin("SUB_FAIL")
                .build());
        metrics.dlq("sub");
    }
}
