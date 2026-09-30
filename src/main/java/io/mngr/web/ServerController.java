package io.mngr.web;

import io.mngr.config.MngrProperties;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.Protocol;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.Page;
import io.mngr.core.model.ServerConfig;
import io.mngr.core.model.ServerRuntime;
import io.mngr.core.model.SubscriptionDef;
import io.mngr.core.model.TopicConfig;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/v1/servers")
@RequiredArgsConstructor
public class ServerController {

    private final ConfigRepository repo;
    private final ServerManager serverManager;
    private final AuditHelper audit;
    private final MngrProperties props;

    @GetMapping
    public ApiResponse<Page<Map<String, Object>>> list(@RequestParam(required = false) String protocol,
                                                       @RequestParam(required = false) String keyword,
                                                       @RequestParam(defaultValue = "0") int page,
                                                       @RequestParam(defaultValue = "20") int size) {
        List<ServerConfig> items = repo.listServers(protocol, keyword, page, size);
        long total = repo.countServers(protocol, keyword);
        return ApiResponse.ok(new Page<>(items.stream().map(this::view).toList(), total));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable String id) {
        return ApiResponse.ok(view(repo.findServer(id)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND, "server 不存在: " + id))));
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> save(@RequestBody ServerConfig in) {
        if (in.getName() == null || in.getName().isBlank()) {
            throw new MngrException(ErrorCode.VALIDATION, "name 必填");
        }
        if (in.getProtocol() == null) {
            throw new MngrException(ErrorCode.VALIDATION, "protocol 必填(KAFKA|MQTT)");
        }
        // Jackson 反序列化不走 @Builder.Default：补空值
        if (in.getConn() == null) in.setConn(new HashMap<>());
        if (in.getCredentials() == null) in.setCredentials(new HashMap<>());
        if (in.getMgmt() == null) in.setMgmt(new HashMap<>());
        if (in.getConnMode() == null) in.setConnMode(io.mngr.core.enums.ConnMode.LAZY);
        boolean isNew = in.getId() == null || in.getId().isBlank();
        if (isNew) {
            in.setId(Protocol.KAFKA == in.getProtocol() ? "kafka-" : "emqx-");
            in.setId(in.getId() + System.currentTimeMillis() % 100000);
            for (ServerConfig x : repo.listServers(in.getProtocol().name(), null, 0, 1000)) {
                if (x.getName().equals(in.getName())) {
                    throw new MngrException(ErrorCode.DUP_NAME, "名称已存在: " + in.getName());
                }
            }
        } else {
            ServerConfig cur = repo.findServer(in.getId())
                    .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
            if (in.getCredentials() == null || in.getCredentials().isEmpty()) {
                in.setCredentials(cur.getCredentials()); // 未提交凭据则保留
            }
            in.setCreatedAt(cur.getCreatedAt());
        }
        in.setUpdatedAt(Instant.now());
        repo.saveServer(in);
        serverManager.applyChange(in);
        audit.record(isNew ? "CREATE" : "UPDATE", "SERVER", in.getId(), null, in);
        return ApiResponse.ok(view(in));
    }

    @PostMapping("/{id}/connect")
    public ApiResponse<Map<String, Object>> connect(@PathVariable String id) {
        repo.findServer(id).orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
        serverManager.connect(id);
        long deadline = System.currentTimeMillis() + 8000;
        ServerRuntime rt = serverManager.status(id);
        while (System.currentTimeMillis() < deadline && rt.getState().name().equals("CONNECTING")) {
            try { Thread.sleep(200); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            rt = serverManager.status(id);
        }
        audit.record("CONNECT", "SERVER", id, null, null);
        return ApiResponse.ok(statusView(id));
    }

    @PostMapping("/{id}/disconnect")
    public ApiResponse<Map<String, Object>> disconnect(@PathVariable String id) {
        serverManager.disconnect(id);
        audit.record("DISCONNECT", "SERVER", id, null, null);
        return ApiResponse.ok(statusView(id));
    }

    @GetMapping("/{id}/status")
    public ApiResponse<Map<String, Object>> status(@PathVariable String id) {
        return ApiResponse.ok(statusView(id));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable String id,
                                    @RequestParam(defaultValue = "false") boolean force) {
        repo.findServer(id).orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
        if (!force) {
            List<String> subs = repo.listSubscriptions(id).stream()
                    .map(SubscriptionDef::getId).toList();
            long topics = repo.countTopics(id, null);
            if (!subs.isEmpty() || topics > 0) {
                throw new MngrException(ErrorCode.IN_USE,
                        "仍被引用: subscriptions=" + subs.size() + ", topics=" + topics);
            }
        }
        serverManager.disconnect(id);
        repo.deleteServer(id);
        audit.record("DELETE", "SERVER", id, null, null);
        return ApiResponse.ok(null);
    }

    Map<String, Object> statusView(String id) {
        ServerRuntime rt = serverManager.status(id);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("serverId", id);
        m.put("state", rt.getState().name());
        m.put("retryCount", rt.getRetryCount());
        m.put("lastError", rt.getLastError());
        m.put("connectedSince", rt.getConnectedSince());
        m.put("protocolInfo", rt.getProtocolInfo());
        return m;
    }

    /** 凭据永不出明文：仅返回 hasCredentials + 键名列表。 */
    Map<String, Object> view(ServerConfig c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", c.getId());
        m.put("name", c.getName());
        m.put("protocol", c.getProtocol().name());
        m.put("enabled", c.isEnabled());
        m.put("connMode", c.getConnMode().name());
        m.put("conn", c.getConn());
        m.put("mgmt", c.getMgmt());
        m.put("tags", c.getTags());
        m.put("credentialKeys", c.getCredentials() == null ? List.of() : new ArrayList<>(c.getCredentials().keySet()));
        m.put("hasCredentials", c.getCredentials() != null && !c.getCredentials().isEmpty());
        m.put("createdAt", c.getCreatedAt());
        m.put("updatedAt", c.getUpdatedAt());
        m.put("runtime", statusView(c.getId()));
        return m;
    }
}
