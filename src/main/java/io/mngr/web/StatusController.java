package io.mngr.web;

import io.mngr.config.MngrProperties;
import io.mngr.core.model.ServerConfig;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.core.sub.SubscriberEngine;
import io.mngr.ws.WsHub;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1/status")
@RequiredArgsConstructor
public class StatusController {

    private final ServerManager serverManager;
    private final ConfigRepository repo;
    private final SubscriberEngine subscriberEngine;
    private final WsHub wsHub;
    private final MngrProperties props;
    private final RuntimeRepository runtime;
    private final io.mngr.core.pub.PublisherEngine publisherEngine;

    @GetMapping("/overview")
    public ApiResponse<Map<String, Object>> overview() {
        Map<String, Object> out = new LinkedHashMap<>();
        List<Map<String, Object>> servers = new ArrayList<>();
        int connected = 0, degraded = 0, down = 0;
        for (ServerConfig c : repo.listServers(null, null, 0, 10000)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", c.getId());
            m.put("name", c.getName());
            m.put("protocol", c.getProtocol().name());
            m.put("enabled", c.isEnabled());
            String st = serverManager.status(c.getId()).getState().name();
            m.put("state", st);
            switch (st) {
                case "CONNECTED" -> connected++;
                case "DEGRADED", "CONNECTING" -> degraded++;
                case "DOWN" -> down++;
                default -> { }
            }
            servers.add(m);
        }
        out.put("servers", servers);
        out.put("serverStats", Map.of("connected", connected, "unstable", degraded, "down", down,
                "total", servers.size()));
        out.put("subscriptions", subscriberEngine.activeStatus());
        out.put("publishBuffers", publisherEngine.bufferSizes());
        out.put("wsSessions", wsHub.sessionCount());
        out.put("storeMode", props.getStore().getMode());
        Set<String> caps = new TreeSet<>();
        runtime.capabilities().forEach(c -> caps.add(c.name()));
        out.put("runtimeCapabilities", caps);
        out.put("allowAdminOps", props.isAllowAdminOps());
        out.put("version", "1.0.0");
        return ApiResponse.ok(out);
    }

    @GetMapping("/audit")
    public ApiResponse<?> audit(@RequestParam(defaultValue = "0") int page,
                                @RequestParam(defaultValue = "20") int size) {
        if (!runtime.capabilities().contains(RuntimeRepository.RuntimeCapability.AUDIT_QUERY)) {
            return new ApiResponse<>(8001, "file 模式不支持审计查询", null, UUID.randomUUID().toString());
        }
        var p = runtime.queryAudit(page, size);
        List<Map<String, Object>> items = new ArrayList<>();
        for (var e : p.getItems()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", e.getId());
            m.put("actor", e.getActor());
            m.put("action", e.getAction());
            m.put("objType", e.getObjType());
            m.put("objId", e.getObjId());
            m.put("beforeJson", e.getBeforeJson());
            m.put("afterJson", e.getAfterJson());
            m.put("createdAt", e.getCreatedAt());
            items.add(m);
        }
        return ApiResponse.ok(Map.of("items", items, "total", p.getTotal()));
    }
}
