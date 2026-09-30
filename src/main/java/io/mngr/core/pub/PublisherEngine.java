package io.mngr.core.pub;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.codec.json.JsonCodec;
import io.mngr.config.MngrProperties;
import io.mngr.core.enums.*;
import io.mngr.core.evt.EventBus;
import io.mngr.core.exception.MngrException;
import io.mngr.core.exception.PublishException;
import io.mngr.core.metrics.MngrMetrics;
import io.mngr.core.model.*;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.spi.ProtocolAdapter;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.core.template.TemplateService;
import io.mngr.core.topic.TopicManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** 发布路径（§7.4）：校验→模板预校验→限流→发送→重试/缓冲/DLQ。 */
@Slf4j
@RequiredArgsConstructor
public class PublisherEngine {

    private final ConfigRepository repo;
    private final ServerManager serverManager;
    private final TopicManager topicManager;
    private final TemplateService templateService;
    private final JsonCodec codec;
    private final MngrMetrics metrics;
    private final RuntimeRepository runtime;
    private final MngrProperties props;
    private final EventBus bus;

    private final Map<String, BufferQueue> buffers = new ConcurrentHashMap<>();
    private final Map<String, RateLimiter> limiters = new ConcurrentHashMap<>();

    private BufferQueue buffer(String serverId) {
        return buffers.computeIfAbsent(serverId,
                id -> new BufferQueue(props.getPublish().getBufferCapacity(), props.getPublish().getOverflow()));
    }

    private RateLimiter limiter(ServerConfig cfg) {
        return limiters.computeIfAbsent(cfg.getId(), id -> new RateLimiter(
                cfg.connInt("rateLimitMsgPerSec", 0),
                cfg.connInt("rateLimitBytesPerSec", 0)));
    }

    public PublishResult publish(PublishRequest req) {
        ServerConfig cfg = serverManager.getConfig(req.getServerId());
        String topic = topicManager.resolvePublishTopic(req.getServerId(), req.getTopicId(), req.getTopic());
        req.setTopic(topic);

        byte[] payload;
        JsonNode node;
        if (req.getPayload() instanceof byte[] b) {
            payload = b;
            node = codec.decode(b);
        } else if (req.getPayload() instanceof String s && s.trim().startsWith("{")) {
            node = codec.decode(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            payload = codec.encode(node);
        } else if (req.getPayload() == null) {
            throw new MngrException(ErrorCode.VALIDATION, "payload 必填");
        } else {
            node = codec.decode(codec.encode(
                    io.mngr.store.StoreJson.mapper().valueToTree(req.getPayload())));
            payload = codec.encode(node);
        }

        // 模板预校验（FR-6.4）
        if (req.getTemplateId() != null && !req.getTemplateId().isBlank()) {
            TemplateDef t = (req.getTemplateVersion() == null
                    ? templateService.get(req.getTemplateId())
                    : templateService.get(req.getTemplateId(), req.getTemplateVersion()))
                    .orElseThrow(() -> new MngrException(ErrorCode.TEMPLATE_NOT_FOUND,
                            "模板不存在: " + req.getTemplateId()));
            List<String> violations = codec.validateAgainstTemplate(node, t.getFields());
            if (!violations.isEmpty()) {
                throw new MngrException(ErrorCode.TEMPLATE_MISMATCH,
                        "payload 与模板不匹配: " + String.join("; ", violations), violations);
            }
            req.headersOrNew().put("x-template", t.getId() + "@" + t.getVersion());
        }
        req.headersOrNew().put("x-codec", "json");
        req.headersOrNew().put("x-msg-id", UUID.randomUUID().toString());

        if (!limiter(cfg).tryAcquire(payload.length)) {
            throw new MngrException(ErrorCode.RATE_LIMITED);
        }

        serverManager.ensureConnected(cfg.getId());
        ProtocolAdapter adapter = serverManager.adapter(cfg.getProtocol());
        try {
            PublishResult r = adapter.publish(req, payload).join();
            if (r.isSuccess()) {
                metrics.publishSuccess(cfg.getProtocol().name());
                return r;
            }
            handleFailure(cfg, req, payload, r.getErrorCode(), r.getErrorMessage());
            return r;
        } catch (PublishException pe) {
            metrics.publishFail(cfg.getProtocol().name());
            handleFailureClassified(cfg, req, payload, pe);
            return PublishResult.builder().success(false)
                    .serverId(cfg.getId()).topic(topic)
                    .errorCode(pe.getErrorCode().code())
                    .errorMessage(pe.getMessage()).build();
        }
    }

    private void handleFailureClassified(ServerConfig cfg, PublishRequest req, byte[] payload, PublishException pe) {
        if (pe.getKind() == FailKind.RETRYABLE || pe.getKind() == FailKind.DISCONNECTED) {
            withInlineRetry(cfg, req, payload);
        } else {
            toDlq(cfg, req, payload, pe.getErrorCode().code(), pe.getMessage());
        }
    }

    private void handleFailure(ServerConfig cfg, PublishRequest req, byte[] payload, Integer code, String msg) {
        metrics.publishFail(cfg.getProtocol().name());
        withInlineRetry(cfg, req, payload);
    }

    /** 同步小重试 ≤3（退避 200/500/1200ms），仍失败 → 缓冲（断连类）或 DLQ。 */
    private void withInlineRetry(ServerConfig cfg, PublishRequest req, byte[] payload) {
        long[] waits = {200, 500, 1200};
        for (long w : waits) {
            try {
                Thread.sleep(w);
                serverManager.ensureConnected(cfg.getId());
                PublishResult r = serverManager.adapter(cfg.getProtocol()).publish(req, payload).join();
                if (r.isSuccess()) {
                    metrics.publishSuccess(cfg.getProtocol().name());
                    return;
                }
            } catch (Exception ignored) { }
        }
        // 终失败：断连状态入缓冲，否则 DLQ
        if (serverManager.status(cfg.getId()).getState() != ServerState.CONNECTED) {
            boolean ok = buffer(cfg.getId()).offer(new BufferQueue.Buffered(req, payload, System.currentTimeMillis(), 0));
            if (!ok) toDlq(cfg, req, payload, ErrorCode.PUBLISH_FAILED.code(), "缓冲溢出丢弃");
            bus.fire("publish_buffered", cfg.getId(), Map.of("topic", req.getTopic(), "bufferSize", buffer(cfg.getId()).size()));
        } else {
            toDlq(cfg, req, payload, ErrorCode.PUBLISH_FAILED.code(), "重试耗尽仍失败");
        }
    }

    private void toDlq(ServerConfig cfg, PublishRequest req, byte[] payload, int code, String msg) {
        if (!props.isH2() && !runtime.capabilities().contains(RuntimeRepository.RuntimeCapability.DLQ_REPLAY)) {
            // file 模式仍可 append（capabilities 只限制查询）
        }
        runtime.appendDlq(DlqRecord.builder()
                .serverId(cfg.getId()).topic(req.getTopic())
                .headers(req.getHeaders()).payload(payload)
                .errCode(code).errMsg(msg).origin("PUB_FAIL")
                .templateId(req.getTemplateId())
                .build());
        metrics.dlq("pub");
    }

    /** 重连成功钩子：排空该 server 缓冲（保持顺序）。 */
    public void drainBuffer(String serverId) {
        BufferQueue bq = buffers.get(serverId);
        if (bq == null) return;
        ServerConfig cfg = repo.findServer(serverId).orElse(null);
        if (cfg == null) return;
        BufferQueue.Buffered b;
        int sent = 0;
        while ((b = bq.poll()) != null) {
            try {
                PublishRequest req = (PublishRequest) b.request();
                PublishResult r = serverManager.adapter(cfg.getProtocol()).publish(req, b.payload()).join();
                if (r.isSuccess()) {
                    sent++;
                    metrics.publishSuccess("buffered");
                } else {
                    toDlq(cfg, req, b.payload(), ErrorCode.PUBLISH_FAILED.code(), "缓冲重发失败");
                }
            } catch (Exception e) {
                toDlq(cfg, (PublishRequest) b.request(), b.payload(),
                        ErrorCode.PUBLISH_FAILED.code(), "缓冲重发异常: " + e.getMessage());
                break; // server 可能又断了，剩余留在下次
            }
        }
        if (sent > 0) {
            bus.fire("publish_drained", serverId, Map.of("sent", sent));
        }
    }

    public Map<String, Integer> bufferSizes() {
        Map<String, Integer> m = new ConcurrentHashMap<>();
        buffers.forEach((k, v) -> m.put(k, v.size()));
        return m;
    }
}
