package io.mngr.adapter.mqtt;

import com.hivemq.client.mqtt.MqttClient;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import com.hivemq.client.mqtt.lifecycle.MqttDisconnectSource;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.auth.Mqtt3SimpleAuth;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import io.mngr.core.enums.*;
import io.mngr.core.exception.PublishException;
import io.mngr.core.model.*;
import io.mngr.core.spi.ClusterOps;
import io.mngr.core.spi.ProtocolAdapter;
import io.mngr.core.spi.TopicAdmin;
import io.mngr.store.StoreJson;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** MQTT Adapter（HiveMQ Client，MQTT 5.0 主路径 + 3.1.1 信封降级，D1）。 */
@Slf4j
public class MqttAdapter implements ProtocolAdapter {

    static class Holder {
        ServerConfig cfg;
        Mqtt5AsyncClient c5;
        Mqtt3AsyncClient c3;
        volatile int maxPacketSize = 262_144;
        /** handle → 实际订阅 filter（用于 unsubscribe）。 */
        final Map<String, String> subs = new ConcurrentHashMap<>();
    }

    private final Map<String, Holder> holders = new ConcurrentHashMap<>();

    @Override
    public Protocol protocol() { return Protocol.MQTT; }

    record HostPort(String host, int port) {}

    static HostPort parseUrl(String url) {
        String s = url.replaceFirst("^(m?qtts?|ssl|wss?|tcp|tls)://", "");
        int slash = s.indexOf('/');
        if (slash >= 0) s = s.substring(0, slash);
        boolean tls = url.startsWith("ssl") || url.startsWith("mqtts") || url.startsWith("wss") || url.startsWith("tls");
        int idx = s.lastIndexOf(':');
        if (idx > 0) {
            try {
                return new HostPort(s.substring(0, idx), Integer.parseInt(s.substring(idx + 1)));
            } catch (NumberFormatException ignored) { }
        }
        return new HostPort(s, tls ? 8883 : 1883);
    }

    @Override
    public void connect(ServerConfig cfg, ConnCallback cb) {
        String url = cfg.connStr("serverUrls", cfg.connStr("host", ""));
        String first = url.split(",")[0].trim();
        if (first.isBlank()) {
            cb.onDisconnected(cfg.getId(), new IllegalArgumentException("serverUrls 未配置"));
            return;
        }
        HostPort hp = parseUrl(first);
        String version = cfg.connStr("protocolVersion", "5");
        String clientId = cfg.connStr("clientId", "mngr-" + cfg.getId().replace(":", "_"));
        String user = cfg.getCredentials().get("username");
        boolean hasAuth = user != null && !user.isBlank();
        int keepAlive = cfg.connInt("keepAlive", 60);

        if (version.startsWith("3")) {
            Mqtt3AsyncClient client = MqttClient.builder().useMqttVersion3()
                    .identifier(clientId)
                    .serverHost(hp.host()).serverPort(hp.port())
                    .addDisconnectedListener(ctx -> onMqttLost(cfg, cb, ctx))
                    .buildAsync();
            var connectStep = client.connectWith().keepAlive(keepAlive)
                    .cleanSession(cfg.connBool("cleanStart", true));
            if (hasAuth) {
                Mqtt3SimpleAuth auth = Mqtt3SimpleAuth.builder()
                        .username(user)
                        .password(ByteBuffer.wrap(
                                cfg.getCredentials().getOrDefault("password", "").getBytes(StandardCharsets.UTF_8)))
                        .build();
                connectStep = connectStep.simpleAuth(auth);
            }
            connectStep.send().whenComplete((ack, err) -> {
                if (err != null) cb.onDisconnected(cfg.getId(), err);
                else {
                    Holder h = new Holder();
                    h.cfg = cfg;
                    h.c3 = client;
                    swap(cfg.getId(), h);
                    cb.onConnected(cfg.getId(), Map.of("protocolVersion", "3.1.1", "clientId", clientId));
                }
            });
        } else {
            Mqtt5AsyncClient client = MqttClient.builder().useMqttVersion5()
                    .identifier(clientId)
                    .serverHost(hp.host()).serverPort(hp.port())
                    .addDisconnectedListener(ctx -> onMqttLost(cfg, cb, ctx))
                    .buildAsync();
            var connectStep = client.connectWith().keepAlive(keepAlive)
                    .cleanStart(cfg.connBool("cleanStart", true))
                    .sessionExpiryInterval(cfg.connInt("sessionExpiry", 300));
            if (hasAuth) {
                Mqtt5SimpleAuth auth = Mqtt5SimpleAuth.builder()
                        .username(user)
                        .password(cfg.getCredentials().getOrDefault("password", "").getBytes(StandardCharsets.UTF_8))
                        .build();
                connectStep = connectStep.simpleAuth(auth);
            }
            connectStep.send().whenComplete((ack, err) -> {
                if (err != null) cb.onDisconnected(cfg.getId(), err);
                else {
                    Holder h = new Holder();
                    h.cfg = cfg;
                    h.c5 = client;
                    int maxPs = ack.getRestrictions().getMaximumPacketSize();
                    if (maxPs > 0) h.maxPacketSize = maxPs;
                    swap(cfg.getId(), h);
                    cb.onConnected(cfg.getId(), Map.of("protocolVersion", "5.0", "clientId", clientId));
                }
            });
        }
    }

    /** 断线回调：主动 disconnect（USER 源或 holder 已移除）时忽略。 */
    private void onMqttLost(ServerConfig cfg, ConnCallback cb, MqttClientDisconnectedContext ctx) {
        if (ctx.getSource() == MqttDisconnectSource.USER) return;
        if (!holders.containsKey(cfg.getId())) return;
        Throwable cause = ctx.getCause();
        cb.onDisconnected(cfg.getId(),
                cause != null ? cause : new java.io.IOException("mqtt connection lost"));
    }

    private void swap(String serverId, Holder fresh) {
        Holder old = holders.put(serverId, fresh);
        if (old != null) closeOld(old);
    }

    private void closeOld(Holder h) {
        try {
            if (h.c5 != null) h.c5.disconnect().get(3, TimeUnit.SECONDS);
            if (h.c3 != null) h.c3.disconnect().get(3, TimeUnit.SECONDS);
        } catch (Exception ignored) { }
    }

    @Override
    public void disconnect(String serverId, Duration drain) {
        Holder h = holders.remove(serverId);
        if (h != null) closeOld(h);
    }

    @Override
    public ServerRuntime status(String serverId) {
        return ServerRuntime.builder().serverId(serverId)
                .state(holders.containsKey(serverId) ? ServerState.CONNECTED : ServerState.INIT).build();
    }

    Holder require(String serverId) {
        Holder h = holders.get(serverId);
        if (h == null) throw PublishException.of(FailKind.DISCONNECTED, ErrorCode.NOT_CONNECTED, "mqtt 未建连");
        return h;
    }

    @Override
    public CompletableFuture<PublishResult> publish(PublishRequest req, byte[] payload) {
        Holder h = require(req.getServerId());
        MqttQos qos = MqttQos.fromCode(req.getQos() == null ? 1 : req.getQos());
        if (payload.length > h.maxPacketSize) {
            throw PublishException.of(FailKind.NON_RETRYABLE, ErrorCode.PACKET_TOO_LARGE,
                    "超过 broker maximum_packet_size " + h.maxPacketSize);
        }
        CompletableFuture<PublishResult> f = new CompletableFuture<>();
        if (h.c5 != null) {
            Mqtt5PublishBuilder.Complete pb = Mqtt5Publish.builder()
                    .topic(req.getTopic())
                    .payload(payload)
                    .qos(qos)
                    .retain(Boolean.TRUE.equals(req.getRetain()));
            List<Mqtt5UserProperty> ups = new ArrayList<>();
            req.headersOrNew().forEach((k, v) -> {
                try { ups.add(Mqtt5UserProperty.of(k, v == null ? "" : v)); } catch (Exception ignored) { }
            });
            if (!ups.isEmpty()) pb = pb.userProperties(Mqtt5UserProperties.of(ups));
            if (req.getMessageExpiryInterval() != null) {
                pb = pb.messageExpiryInterval(req.getMessageExpiryInterval());
            }
            if (req.getCorrelationId() != null) {
                pb = pb.responseTopic("mngr/rpc/" + req.getServerId())
                        .correlationData(req.getCorrelationId().getBytes(StandardCharsets.UTF_8));
            }
            h.c5.publish(pb.build()).whenComplete((res, err) -> {
                if (err != null) f.completeExceptionally(classifyMqtt(err));
                else {
                    f.complete(PublishResult.builder().success(true)
                            .serverId(req.getServerId()).topic(req.getTopic())
                            .messageId(req.headersOrNew().get("x-msg-id"))
                            .protocolMeta(Map.of("reasonCode", "SUCCESS", "qos", qos.getCode()))
                            .build());
                }
            });
        } else {
            byte[] out = wrapEnvelope(req.headersOrNew(), payload);
            h.c3.publishWith()
                    .topic(req.getTopic())
                    .payload(out)
                    .qos(qos)
                    .retain(Boolean.TRUE.equals(req.getRetain()))
                    .send().whenComplete((res, err) -> {
                        if (err != null) f.completeExceptionally(classifyMqtt(err));
                        else f.complete(PublishResult.builder().success(true)
                                .serverId(req.getServerId()).topic(req.getTopic())
                                .protocolMeta(Map.of("qos", qos.getCode(), "protocol", "3.1.1"))
                                .build());
                    });
        }
        return f.orTimeout(15, TimeUnit.SECONDS).exceptionally(ex -> PublishResult.builder().success(false)
                .serverId(req.getServerId()).topic(req.getTopic())
                .errorCode(ErrorCode.PUBLISH_FAILED.code())
                .errorMessage(String.valueOf(unwrap(ex).getMessage())).build());
    }

    static Throwable unwrap(Throwable e) {
        return e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
    }

    /** 3.1.1 无 user properties：信封 {"_h":{...},"_p":"<payload string>"}。 */
    static byte[] wrapEnvelope(Map<String, String> headers, byte[] payload) {
        if (headers == null || headers.isEmpty()) return payload;
        try {
            var root = StoreJson.mapper().createObjectNode();
            var hs = root.putObject("_h");
            headers.forEach(hs::put);
            root.put("_p", new String(payload, StandardCharsets.UTF_8));
            return root.toString().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            return payload;
        }
    }

    /** 消费侧信封探测解包（3.1.1 兼容两种格式）。 */
    static RawIncoming unwrapEnvelope(RawIncoming in) {
        byte[] p = in.getPayload();
        if (p == null || p.length < 8 || p[0] != '{') return in;
        try {
            var node = StoreJson.mapper().readTree(p);
            if (node.isObject() && node.has("_h") && node.has("_p")) {
                Map<String, String> hs = new HashMap<>(in.getHeaders());
                node.get("_h").fields().forEachRemaining(e -> hs.put(e.getKey(), e.getValue().asText()));
                byte[] payload = node.get("_p").isTextual()
                        ? node.get("_p").asText().getBytes(StandardCharsets.UTF_8)
                        : node.get("_p").toString().getBytes(StandardCharsets.UTF_8);
                return RawIncoming.builder()
                        .serverId(in.getServerId()).subscriptionId(in.getSubscriptionId())
                        .topic(in.getTopic()).payload(payload)
                        .headers(hs).protocolMeta(in.getProtocolMeta()).receiveTs(in.getReceiveTs())
                        .build();
            }
        } catch (Exception ignored) { }
        return in;
    }

    static PublishException classifyMqtt(Throwable e) {
        Throwable t = unwrap(e);
        String msg = String.valueOf(t.getMessage());
        if (t instanceof TimeoutException || msg.toLowerCase().contains("timeout")) {
            return new PublishException(FailKind.TIMEOUT, ErrorCode.PUBLISH_FAILED, msg, t);
        }
        if (t instanceof java.io.IOException || msg.toLowerCase().contains("connect")
                || msg.toLowerCase().contains("closed")) {
            return new PublishException(FailKind.DISCONNECTED, ErrorCode.NOT_CONNECTED, msg, t);
        }
        return new PublishException(FailKind.NON_RETRYABLE, ErrorCode.PUBLISH_FAILED, msg, t);
    }

    static String shareFilter(String topic, String group) {
        if (group == null || group.isBlank()) return topic;
        if (topic.startsWith("$share/")) return topic;
        return "$share/" + group + "/" + topic;
    }

    @Override
    public String startSubscription(SubscriptionDef def, Consumer<RawIncoming> sink) {
        Holder h = require(def.getServerId());
        String filter = shareFilter(def.getTopic(), def.getGroup());
        MqttQos qos = MqttQos.fromCode(def.getQos() == null ? 1 : def.getQos());
        if (h.c5 != null) {
            h.c5.subscribeWith()
                    .topicFilter(filter)
                    .qos(qos)
                    .callback((Mqtt5Publish p) -> {
                        Map<String, String> hs = new HashMap<>();
                        p.getUserProperties().asList().forEach(up ->
                                hs.put(up.getName().toString(), up.getValue().toString()));
                        Map<String, Object> meta = new HashMap<>();
                        meta.put("qos", p.getQos().getCode());
                        meta.put("retain", p.isRetain());
                        p.getMessageExpiryInterval().ifPresent(v -> meta.put("messageExpiryInterval", v));
                        sink.accept(RawIncoming.builder()
                                .serverId(h.cfg.getId()).subscriptionId(def.getId())
                                .topic(p.getTopic().toString()).payload(p.getPayloadAsBytes())
                                .headers(hs).protocolMeta(meta).receiveTs(System.currentTimeMillis()).build());
                    })
                    .send();
        } else {
            h.c3.subscribeWith()
                    .topicFilter(filter)
                    .qos(qos)
                    .callback((Mqtt3Publish p) -> {
                        Map<String, Object> meta = new HashMap<>();
                        meta.put("qos", p.getQos().getCode());
                        meta.put("retain", p.isRetain());
                        sink.accept(unwrapEnvelope(RawIncoming.builder()
                                .serverId(h.cfg.getId()).subscriptionId(def.getId())
                                .topic(p.getTopic().toString()).payload(p.getPayloadAsBytes())
                                .headers(new HashMap<>()).protocolMeta(meta)
                                .receiveTs(System.currentTimeMillis()).build()));
                    })
                    .send();
        }
        String handle = "msub-" + def.getId();
        h.subs.put(handle, filter);
        return handle;
    }

    @Override
    public void stopSubscription(String serverId, String handle) {
        Holder h = holders.get(serverId);
        if (h == null) return;
        String filter = h.subs.remove(handle);
        if (filter == null) return;
        try {
            if (h.c5 != null) {
                h.c5.unsubscribeWith().topicFilter(filter).send().get(5, TimeUnit.SECONDS);
            } else if (h.c3 != null) {
                h.c3.unsubscribeWith().topicFilter(filter).send().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("unsubscribe failed server={} filter={}: {}", serverId, filter, e.toString());
        }
    }

    @Override
    public void pause(String subscriptionId) {
        // MQTT 无原生暂停：由 SubscriberEngine 队列门控（开发计划偏差已声明）
    }

    @Override
    public void resume(String subscriptionId) { }

    @Override
    public boolean supports(Capability cap) {
        return switch (cap) {
            case SHARED_SUBSCRIPTION, RETAIN, QOS, MESSAGE_EXPIRY, SUBSCRIPTION_ID, RPC_REQUEST, CLUSTER_OPS -> true;
            default -> false;
        };
    }

    @Override
    public Optional<TopicAdmin> topicAdmin(String serverId) {
        return Optional.empty(); // MQTT 无 broker 侧 topic 创建概念（FR-2.2）
    }

    @Override
    public Optional<ClusterOps> clusterOps(String serverId) {
        Holder h = holders.get(serverId);
        if (h == null) return Optional.empty();
        String baseUrl = h.cfg.mgmtObj("baseUrl") == null ? null : String.valueOf(h.cfg.mgmtObj("baseUrl"));
        String key = h.cfg.getCredentials().get("mgmtApiKey");
        String secret = h.cfg.getCredentials().get("mgmtSecret");
        return Optional.of(new EmqxClusterOps(h, baseUrl, key, secret));
    }

    // 供 EmqxClusterOps 复用发布通道清除 retained
    Holder holderOf(String serverId) { return holders.get(serverId); }

    Mqtt5AsyncClient client5(String serverId) {
        Holder h = holders.get(serverId);
        return h == null ? null : h.c5;
    }
}
