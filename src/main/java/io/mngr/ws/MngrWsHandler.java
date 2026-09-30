package io.mngr.ws;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.model.PublishRequest;
import io.mngr.core.pub.PublisherEngine;
import io.mngr.store.StoreJson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** WS 控制帧：subscribe / unsubscribe / publish_try / watch_status（§6.3）。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MngrWsHandler extends TextWebSocketHandler {

    private final WsHub hub;
    private final PublisherEngine publisherEngine;
    private final ExecutorService exec = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mngr-ws-worker");
        t.setDaemon(true);
        return t;
    });

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        hub.open(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        hub.close(session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        WsHub.WsSession s = hub.session(session.getId());
        if (s == null) return;
        try {
            JsonNode f = StoreJson.mapper().readTree(message.getPayload());
            String action = f.path("action").asText("");
            int wsId = f.path("wsId").asInt(0);
            switch (action) {
                case "subscribe" -> {
                    s.subs.add(new WsHub.Sub(f.path("serverId").asText(), f.path("topic").asText("")));
                    ack(session, wsId, action);
                }
                case "unsubscribe" -> {
                    String srv = f.path("serverId").asText();
                    s.subs.removeIf(sub -> sub.serverId().equals(srv)
                            && (f.path("topic").isMissingNode() || sub.topicFilter().equals(f.path("topic").asText())));
                    ack(session, wsId, action);
                }
                case "watch_status" -> {
                    s.watching = true;
                    ack(session, wsId, action);
                }
                case "publish_try" -> {
                    PublishRequest req = StoreJson.mapper().treeToValue(f.path("request"), PublishRequest.class);
                    exec.submit(() -> {
                        Map<String, Object> frame = new LinkedHashMap<>();
                        frame.put("type", "result");
                        frame.put("wsId", wsId);
                        try {
                            frame.put("data", publisherEngine.publish(req));
                        } catch (Exception e) {
                            frame.put("error", Map.of("message", String.valueOf(e.getMessage())));
                        }
                        hub.sendTo(session.getId(), frame);
                    });
                }
                default -> ack(session, wsId, "unknown:" + action);
            }
        } catch (Exception e) {
            log.debug("ws frame error: {}", e.toString());
            hub.sendTo(session.getId(), Map.of("type", "result", "wsId", 0,
                    "error", Map.of("message", String.valueOf(e.getMessage()))));
        }
    }

    private void ack(WebSocketSession session, int wsId, String action) {
        hub.sendTo(session.getId(), Map.of("type", "ack", "wsId", wsId, "action", action));
    }
}
