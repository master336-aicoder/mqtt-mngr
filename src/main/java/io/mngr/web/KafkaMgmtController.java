package io.mngr.web;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.Protocol;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.ServerConfig;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ClusterOps;
import io.mngr.core.spi.TopicAdmin;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** Kafka 集群管理面 REST（FR-2.4 K1~K18 受控子集）。 */
@RestController
@RequestMapping("/api/v1/mgmt/kafka/{serverId}")
@RequiredArgsConstructor
public class KafkaMgmtController {

    private final ServerManager serverManager;
    private final MgmtGuard guard;
    private final AuditHelper audit;

    private TopicAdmin topicAdmin(String serverId) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        if (cfg.getProtocol() != Protocol.KAFKA) {
            throw MngrException.of(ErrorCode.VALIDATION, "server %s 非 Kafka 协议", serverId);
        }
        serverManager.ensureConnected(serverId);
        return serverManager.adapter(Protocol.KAFKA).topicAdmin(serverId)
                .orElseThrow(() -> MngrException.of(ErrorCode.NOT_CONNECTED, "Kafka 管理面不可用"));
    }

    private ClusterOps clusterOps(String serverId) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        if (cfg.getProtocol() != Protocol.KAFKA) {
            throw MngrException.of(ErrorCode.VALIDATION, "server %s 非 Kafka 协议", serverId);
        }
        serverManager.ensureConnected(serverId);
        return serverManager.adapter(Protocol.KAFKA).clusterOps(serverId)
                .orElseThrow(() -> MngrException.of(ErrorCode.NOT_CONNECTED, "Kafka 管理面不可用"));
    }

    // ---------------- 只读 ----------------

    @GetMapping("/overview")
    public ApiResponse<Map<String, Object>> overview(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(clusterOps(serverId).overview());
    }

    @GetMapping("/topics")
    public ApiResponse<List<Map<String, Object>>> topics(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(clusterOps(serverId).listTopicsDetail());
    }

    @GetMapping("/topics/{topic}")
    public ApiResponse<Map<String, Object>> topicDetail(@PathVariable String serverId,
                                                        @PathVariable String topic) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(clusterOps(serverId).describeTopic(topic));
    }

    @GetMapping("/topics/{topic}/stats")
    public ApiResponse<Map<String, Object>> topicStats(@PathVariable String serverId,
                                                       @PathVariable String topic) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(clusterOps(serverId).topicStats(topic));
    }

    @GetMapping("/groups")
    public ApiResponse<List<Map<String, Object>>> groups(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(clusterOps(serverId).listGroups());
    }

    @GetMapping("/groups/{gid}")
    public ApiResponse<Map<String, Object>> groupDetail(@PathVariable String serverId,
                                                        @PathVariable String gid) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(clusterOps(serverId).describeGroup(gid));
    }

    // ---------------- 写操作 ----------------

    @PostMapping("/topics/_create")
    public ApiResponse<Map<String, Object>> createTopic(@PathVariable String serverId,
                                                        @RequestBody Map<String, Object> body) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        guard.requireWrite(cfg);
        String name = str(body, "name");
        if (name == null || name.isBlank()) throw MngrException.of(ErrorCode.VALIDATION, "name 必填");
        int partitions = intOr(body, "partitions", 1);
        short rf = (short) intOr(body, "replicationFactor", 1);
        @SuppressWarnings("unchecked")
        Map<String, String> config = body.get("config") instanceof Map<?, ?> m
                ? (Map<String, String>) m : Map.of();
        topicAdmin(serverId).createTopic(name, partitions, rf, config);
        audit.record("MGMT_KAFKA_CREATE_TOPIC", "server", serverId, null,
                Map.of("topic", name, "partitions", partitions));
        return ApiResponse.ok(Map.of("topic", name, "created", true));
    }

    @PostMapping("/topics/{topic}/_add-partitions")
    public ApiResponse<Map<String, Object>> addPartitions(@PathVariable String serverId,
                                                          @PathVariable String topic,
                                                          @RequestBody Map<String, Object> body) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        guard.requireWrite(cfg);
        int total = intOr(body, "totalPartitions", 0);
        if (total <= 0) throw MngrException.of(ErrorCode.VALIDATION, "totalPartitions 必须大于 0");
        topicAdmin(serverId).addPartitions(topic, total);
        audit.record("MGMT_KAFKA_ADD_PARTITIONS", "server", serverId, null,
                Map.of("topic", topic, "totalPartitions", total));
        return ApiResponse.ok(Map.of("topic", topic, "totalPartitions", total));
    }

    /** DANGER：删除 topic 需 confirm 回显 topic 名。 */
    @DeleteMapping("/topics/{topic}")
    public ApiResponse<Map<String, Object>> deleteTopic(@PathVariable String serverId,
                                                        @PathVariable String topic,
                                                        @RequestParam(required = false) String confirm) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        guard.requireWrite(cfg);
        topicAdmin(serverId); // 连接校验先于 confirm，避免误导性错误
        guard.requireDanger(cfg, topic, confirm);
        topicAdmin(serverId).deleteTopic(topic);
        audit.record("MGMT_KAFKA_DELETE_TOPIC", "server", serverId, Map.of("topic", topic), null);
        return ApiResponse.ok(Map.of("topic", topic, "deleted", true));
    }

    @PostMapping("/groups/{gid}/_reset-offsets")
    public ApiResponse<Map<String, Object>> resetOffsets(@PathVariable String serverId,
                                                        @PathVariable String gid,
                                                        @RequestParam(required = false) String confirm,
                                                        @RequestBody Map<String, Object> body) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        guard.requireWrite(cfg);
        String topic = str(body, "topic");
        String strategy = str(body, "strategy");
        if (topic == null || strategy == null) {
            throw MngrException.of(ErrorCode.VALIDATION, "topic/strategy 必填");
        }
        // DANGER：重置 offset 需 confirm 回显消费组
        guard.requireDanger(cfg, gid, confirm);
        Long ts = body.get("timestamp") == null ? null : Long.valueOf(String.valueOf(body.get("timestamp")));
        Map<String, Object> r = clusterOps(serverId).resetOffsets(gid, topic, strategy, ts);
        audit.record("MGMT_KAFKA_RESET_OFFSETS", "server", serverId, null,
                Map.of("group", gid, "topic", topic, "strategy", strategy));
        return ApiResponse.ok(r);
    }

    private static String str(Map<String, Object> m, String k) {
        return m.get(k) == null ? null : String.valueOf(m.get(k));
    }

    private static int intOr(Map<String, Object> m, String k, int def) {
        Object v = m.get(k);
        if (v == null) return def;
        try { return Integer.parseInt(String.valueOf(v)); } catch (NumberFormatException e) { return def; }
    }
}
