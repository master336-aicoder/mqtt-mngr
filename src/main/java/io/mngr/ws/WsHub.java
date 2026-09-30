package io.mngr.ws;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.codec.json.JsonCodec;
import io.mngr.config.MngrProperties;
import io.mngr.core.evt.EventBus;
import io.mngr.core.model.MessageContext;
import io.mngr.core.sub.SubscriberEngine;
import io.mngr.store.StoreJson;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** WS 会话中心：调试订阅、消息推流、状态事件（§6.3）。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WsHub {

    public static class WsSession {
        public final WebSocketSession raw;
        public final Set<Sub> subs = ConcurrentHashMap.newKeySet();
        public volatile boolean watching = false;
        /** 简易背压：单帧在途，忙则丢弃（§6.3 调试通道丢帧语义）。 */
        public final java.util.concurrent.atomic.AtomicBoolean sending =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        public final java.util.concurrent.atomic.AtomicLong dropped = new java.util.concurrent.atomic.AtomicLong();

        public WsSession(WebSocketSession raw) { this.raw = raw; }
    }

    public record Sub(String serverId, String topicFilter) {}

    private final Map<String, WsSession> sessions = new ConcurrentHashMap<>();
    private final SubscriberEngine subscriberEngine;
    private final EventBus eventBus;
    private final MngrProperties props;

    @PostConstruct
    public void init() {
        subscriberEngine.registerDebugSink(this::pushMessage);
        eventBus.register(this::pushEvent);
    }

    public WsSession open(WebSocketSession s) {
        WsSession w = new WsSession(s);
        sessions.put(s.getId(), w);
        return w;
    }

    public void close(String sessionId) {
        sessions.remove(sessionId);
    }

    void send(WsSession s, Map<String, Object> frame) {
        WebSocketSession raw = s.raw;
        try {
            if (!raw.isOpen()) return;
            if (!s.sending.compareAndSet(false, true)) {
                s.dropped.incrementAndGet();
                log.debug("ws backpressure: drop frame session={}", raw.getId());
                return; // 调试通道丢帧（§6.3）
            }
            try {
                raw.sendMessage(new TextMessage(StoreJson.mapper().writeValueAsString(frame)));
            } finally {
                s.sending.set(false);
            }
        } catch (Exception e) {
            log.debug("ws send failed: {}", e.toString());
        }
    }

    void pushMessage(MessageContext ctx) {
        for (WsSession s : sessions.values()) {
            boolean hit = s.subs.stream().anyMatch(sub ->
                    sub.serverId().equals(ctx.getServerId())
                            && (sub.topicFilter().isBlank()
                                || io.mngr.core.topic.WildcardMatcher.matches(sub.topicFilter(), ctx.getTopic())));
            if (!hit) continue;
            Map<String, Object> frame = new java.util.LinkedHashMap<>();
            frame.put("type", "msg_frame");
            frame.put("serverId", ctx.getServerId());
            frame.put("subscriptionId", ctx.getSubscriptionId());
            frame.put("topic", ctx.getTopic());
            frame.put("templateId", ctx.getTemplateId());
            frame.put("templateVersion", ctx.getTemplateVersion());
            frame.put("matchType", ctx.getMatchType().name());
            frame.put("matchScore", ctx.getMatchScore());
            frame.put("headers", ctx.getHeaders());
            frame.put("protocolMeta", ctx.getProtocolMeta());
            frame.put("receiveTs", ctx.getReceiveTs());
            if (ctx.isParsed()) {
                frame.put("payloadJson", ctx.getRaw().toString());
                if (ctx.getTyped() != null) {
                    frame.put("typedJson", ctx.getTyped().getValues().toString());
                    if (!ctx.getTyped().getUnknownFields().isEmpty()) {
                        frame.put("unknownFields", ctx.getTyped().getUnknownFields());
                    }
                }
            } else {
                frame.put("parseError", ctx.getParseError());
                frame.put("payloadB64", java.util.Base64.getEncoder().encodeToString(
                        ctx.getRawBytes() == null ? new byte[0] : ctx.getRawBytes()));
            }
            send(s, frame);
        }
    }

    void pushEvent(EventBus.MngrEvent e) {
        for (WsSession s : sessions.values()) {
            if (!s.watching) continue;
            send(s, Map.of("type", "event",
                    "event", e.type(),
                    "subject", e.subject() == null ? "" : e.subject(),
                    "data", e.data() == null ? Map.of() : e.data(),
                    "ts", System.currentTimeMillis()));
        }
    }

    public WsSession session(String sessionId) { return sessions.get(sessionId); }

    public void sendTo(String sessionId, Map<String, Object> frame) {
        WsSession s = sessions.get(sessionId);
        if (s != null) send(s, frame);
    }

    public int sessionCount() { return sessions.size(); }

    public JsonNode node(String json) throws Exception {
        return StoreJson.mapper().readTree(json);
    }
}
