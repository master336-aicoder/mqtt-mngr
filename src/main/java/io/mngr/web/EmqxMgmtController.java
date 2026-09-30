package io.mngr.web;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.Protocol;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.ServerConfig;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ClusterOps;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** EMQX 管理面 REST（FR-2.5 E1~E13 子集）。无 mgmt 凭据时按 supportedOperations 显式降级。 */
@RestController
@RequestMapping("/api/v1/mgmt/emqx/{serverId}")
@RequiredArgsConstructor
public class EmqxMgmtController {

    private final ServerManager serverManager;
    private final MgmtGuard guard;
    private final AuditHelper audit;

    private ClusterOps ops(String serverId) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        if (cfg.getProtocol() != Protocol.MQTT) {
            throw MngrException.of(ErrorCode.VALIDATION, "server %s 非 MQTT 协议", serverId);
        }
        serverManager.ensureConnected(serverId);
        return serverManager.adapter(Protocol.MQTT).clusterOps(serverId)
                .orElseThrow(() -> MngrException.of(ErrorCode.NOT_CONNECTED, "MQTT 管理面不可用"));
    }

    @GetMapping("/operations")
    public ApiResponse<Map<String, Object>> operations(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(Map.of("supported", ops(serverId).supportedOperations()));
    }

    @GetMapping("/overview")
    public ApiResponse<Map<String, Object>> overview(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(ops(serverId).overview());
    }

    @GetMapping("/nodes")
    public ApiResponse<List<Map<String, Object>>> nodes(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(ops(serverId).listNodes());
    }

    @GetMapping("/topics")
    public ApiResponse<List<Map<String, Object>>> topics(@PathVariable String serverId) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(ops(serverId).listTopicsDetail());
    }

    /** MQTT topic 含 '/'，用查询参数传递。 */
    @GetMapping("/topics/detail")
    public ApiResponse<Map<String, Object>> topicDetail(@PathVariable String serverId,
                                                        @RequestParam String topic) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(ops(serverId).describeTopic(topic));
    }

    @GetMapping("/clients")
    public ApiResponse<List<Map<String, Object>>> clients(@PathVariable String serverId,
                                                          @RequestParam(required = false) String filter,
                                                          @RequestParam(defaultValue = "50") int limit) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(ops(serverId).listClients(filter, limit));
    }

    @GetMapping("/subscriptions")
    public ApiResponse<List<Map<String, Object>>> subscriptions(@PathVariable String serverId,
                                                                @RequestParam(required = false) String filter,
                                                                @RequestParam(defaultValue = "50") int limit) {
        guard.requireWrite(serverManager.getConfig(serverId));
        return ApiResponse.ok(ops(serverId).listSubscriptions(filter, limit));
    }

    /** E7 DANGER：清除 retained 消息，confirm 回显 topic（body 或 query 均可）。 */
    @DeleteMapping("/retained")
    public ApiResponse<Map<String, Object>> clearRetained(@PathVariable String serverId,
                                                          @RequestParam(required = false) String topic,
                                                          @RequestParam(required = false) String confirm,
                                                          @RequestBody(required = false) Map<String, String> body) {
        ServerConfig cfg = serverManager.getConfig(serverId);
        guard.requireWrite(cfg);
        String t = topic != null ? topic : (body == null ? null : body.get("topic"));
        String c = confirm != null ? confirm : (body == null ? null : body.get("confirm"));
        if (t == null || t.isBlank()) throw MngrException.of(ErrorCode.VALIDATION, "topic 必填");
        guard.requireDanger(cfg, t, c);
        Map<String, Object> r = ops(serverId).clearRetained(t);
        audit.record("MGMT_EMQX_CLEAR_RETAINED", "server", serverId, Map.of("topic", t), r);
        return ApiResponse.ok(r);
    }
}
