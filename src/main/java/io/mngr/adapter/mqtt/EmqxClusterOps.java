package io.mngr.adapter.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.spi.ClusterOps;
import io.mngr.store.StoreJson;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * EMQX 5.x Dashboard REST 管理（E1~E13 子集）+ 客户端侧能力（E7 retained 清除走
 * publish retain+空 payload，标准 MQTT 语义）。
 * 无 mgmt baseUrl/凭据时自动降级为仅客户端侧操作。
 */
@Slf4j
public class EmqxClusterOps implements ClusterOps {

    private final MqttAdapter.Holder holder;
    private final String baseUrl;   // e.g. http://host:18083
    private final String basicAuth; // base64(key:secret)
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3)).build();

    public EmqxClusterOps(MqttAdapter.Holder holder, String baseUrl, String apiKey, String secret) {
        this.holder = holder;
        this.baseUrl = baseUrl == null || baseUrl.isBlank() ? null : baseUrl.replaceAll("/+$", "");
        if (this.baseUrl != null && apiKey != null && secret != null) {
            this.basicAuth = Base64.getEncoder()
                    .encodeToString((apiKey + ":" + secret).getBytes(StandardCharsets.UTF_8));
        } else {
            this.basicAuth = null;
        }
    }

    JsonNode get(String path) {
        return call("GET", path, null);
    }

    JsonNode call(String method, String path, String body) {
        if (baseUrl == null || basicAuth == null) {
            throw new UnsupportedOperationException("EMQX 管理 API 未配置（mgmt.baseUrl + mgmtApiKey/Secret）");
        }
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(5))
                    .header("Authorization", "Basic " + basicAuth);
            if ("DELETE".equals(method)) b.DELETE();
            else if ("POST".equals(method)) b.POST(HttpRequest.BodyPublishers.ofString(body == null ? "{}" : body));
            else b.GET();
            HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("EMQX API " + resp.statusCode() + ": " + resp.body());
            }
            return StoreJson.mapper().readTree(resp.body());
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("EMQX API " + path + " 失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> overview() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("protocol", "MQTT");
        out.put("clientId", holder.cfg.connStr("clientId", ""));
        if (baseUrl != null && basicAuth != null) {
            JsonNode ov = get("/api/v5/overview");
            out.put("emqxVersion", ov.path("node_version").asText(null));
            out.put("nodeClusterStatus", ov.path("node_cluster_status").asText(null));
            out.put("connections", ov.path("connections").asLong());
            out.put("topics", ov.path("topics").asLong());
            out.put("subscriptions", ov.path("subscriptions").asLong());
            out.put("messagesReceived", ov.path("messages_received").asLong());
            out.put("messagesSent", ov.path("messages_sent").asLong());
            out.put("retained", ov.path("retained").asLong());
        } else {
            out.put("degraded", "无管理 API 凭据，仅客户端侧能力");
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listNodes() {
        if (baseUrl == null) return List.of();
        List<Map<String, Object>> out = new ArrayList<>();
        for (JsonNode n : get("/api/v5/nodes")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("node", n.path("node").asText());
            m.put("uptimeSeconds", n.path("uptime").asLong());
            m.put("connections", n.path("connections").asLong());
            m.put("version", n.path("version").asText(null));
            m.put("memoryUsed", n.path("memory_used").asText(null));
            out.add(m);
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listTopicsDetail() {
        if (baseUrl == null) return List.of();
        List<Map<String, Object>> out = new ArrayList<>();
        JsonNode resp = get("/api/v5/topics?limit=100");
        for (JsonNode t : resp.path("data")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("topic", t.path("topic").asText());
            m.put("subscriberCount", t.path("subscriber_count").asLong());
            m.put("messagesIn", t.path("messages_in").asLong());
            out.add(m);
        }
        return out;
    }

    @Override
    public Map<String, Object> describeTopic(String topic) {
        if (baseUrl == null) return Map.of("topic", topic, "degraded", true);
        JsonNode t = get("/api/v5/topics/" + URIenc(topic));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("topic", t.path("topic").asText());
        m.put("subscriberCount", t.path("subscriber_count").asLong());
        m.put("retained", t.path("retained").asLong());
        return m;
    }

    @Override
    public Map<String, Object> topicStats(String topic) {
        return describeTopic(topic);
    }

    @Override
    public List<Map<String, Object>> listClients(String filter, int limit) {
        if (baseUrl == null) return List.of();
        String q = "/api/v5/clients?limit=" + Math.max(1, Math.min(limit, 100));
        if (filter != null && !filter.isBlank()) q += "&like_clientid=" + URIenc(filter);
        List<Map<String, Object>> out = new ArrayList<>();
        for (JsonNode c : get(q).path("data")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("clientId", c.path("clientid").asText());
            m.put("connected", c.path("connected").asBoolean());
            m.put("ipAddress", c.path("ip_address").asText(null));
            m.put("protoVer", c.path("proto_ver").asInt());
            m.put("keepalive", c.path("keepalive").asInt());
            m.put("subscriptions", c.path("subscriptions").asLong());
            m.put("connectedAt", c.path("connected_at").asText(null));
            out.add(m);
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listSubscriptions(String filter, int limit) {
        if (baseUrl == null) return List.of();
        String q = "/api/v5/subscriptions?limit=" + Math.max(1, Math.min(limit, 100));
        if (filter != null && !filter.isBlank()) q += "&topic=" + URIenc(filter);
        List<Map<String, Object>> out = new ArrayList<>();
        for (JsonNode s : get(q).path("data")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("clientId", s.path("clientid").asText());
            m.put("topic", s.path("topic").asText());
            m.put("qos", s.path("qos").asInt());
            m.put("shareGroup", s.path("share").asText(null));
            out.add(m);
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listGroups() {
        if (baseUrl == null) return List.of();
        List<Map<String, Object>> out = new ArrayList<>();
        for (JsonNode g : get("/api/v5/mqtt/_consumer_groups").path("data")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("group", g.path("group").asText());
            out.add(m);
        }
        return out;
    }

    @Override
    public Map<String, Object> describeGroup(String groupId) {
        return Map.of("groupId", groupId, "note", "EMQX 消费组语义有限，详见订阅列表");
    }

    @Override
    public Map<String, Object> resetOffsets(String groupId, String topic, String strategy, Long timestamp) {
        throw new UnsupportedOperationException("MQTT 无 offset 语义");
    }

    /** E7：标准 MQTT 语义 —— 向目标 topic 发布 retain=true 空载荷。 */
    @Override
    public Map<String, Object> clearRetained(String topic) {
        if (holder.c5 == null) throw new UnsupportedOperationException("仅 MQTT5 支持");
        try {
            holder.c5.publishWith()
                    .topic(topic)
                    .payload(new byte[0])
                    .retain(true)
                    .send().get(5, java.util.concurrent.TimeUnit.SECONDS);
            return Map.of("ok", true, "topic", topic, "method", "retain-empty");
        } catch (Exception e) {
            return Map.of("ok", false, "error", String.valueOf(e.getMessage()));
        }
    }

    @Override
    public Set<String> supportedOperations() {
        if (baseUrl != null && basicAuth != null) {
            return Set.of("overview", "listNodes", "listTopicsDetail", "describeTopic",
                    "listClients", "listSubscriptions", "clearRetained");
        }
        return Set.of("clearRetained");
    }

    static String URIenc(String s) {
        return java.net.URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
