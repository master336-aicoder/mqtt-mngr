package io.mngr.web;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.SubscriptionDef;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.sub.SubscriberEngine;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/v1/subscriptions")
@RequiredArgsConstructor
public class SubscriptionController {

    private final ConfigRepository repo;
    private final SubscriberEngine engine;
    private final AuditHelper audit;

    @Data
    public static class SaveReq {
        private SubscriptionDef subscription;
        private boolean start = false;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list(@RequestParam(required = false) String serverId) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (SubscriptionDef d : repo.listSubscriptions(serverId)) {
            out.add(view(d));
        }
        return ApiResponse.ok(out);
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> save(@RequestBody SaveReq req) {
        SubscriptionDef d = req.getSubscription();
        if (d == null || d.getServerId() == null) {
            throw new MngrException(ErrorCode.VALIDATION, "subscription.serverId 必填");
        }
        if (d.getTopicId() == null && (d.getTopic() == null || d.getTopic().isBlank())) {
            throw new MngrException(ErrorCode.VALIDATION, "topic 与 topicId 至少填一");
        }
        boolean isNew = d.getId() == null || d.getId().isBlank();
        if (isNew) {
            d.setId("sub-" + UUID.randomUUID().toString().substring(0, 8));
            d.setState("STOPPED");
            d.setCreatedAt(Instant.now());
        } else {
            repo.findSubscription(d.getId()).orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
            if (engine.isRunning(d.getId())) {
                throw new MngrException(ErrorCode.IN_USE, "运行中订阅不可编辑，请先停止");
            }
        }
        repo.saveSubscription(d);
        audit.record(isNew ? "CREATE" : "UPDATE", "SUBSCRIPTION", d.getId(), null, d);
        if (req.isStart()) engine.start(d.getId());
        return ApiResponse.ok(view(repo.findSubscription(d.getId()).orElse(d)));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable String id) {
        SubscriptionDef d = repo.findSubscription(id)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
        try { engine.stop(id); } catch (Exception ignored) { }
        repo.deleteSubscription(id);
        audit.record("DELETE", "SUBSCRIPTION", id, d, null);
        return ApiResponse.ok(null);
    }

    @PostMapping("/{id}/start")
    public ApiResponse<Map<String, Object>> start(@PathVariable String id) {
        engine.start(id);
        audit.record("START", "SUBSCRIPTION", id, null, null);
        return ApiResponse.ok(view(repo.findSubscription(id).orElseThrow()));
    }

    @PostMapping("/{id}/stop")
    public ApiResponse<Map<String, Object>> stop(@PathVariable String id) {
        engine.stop(id);
        audit.record("STOP", "SUBSCRIPTION", id, null, null);
        return ApiResponse.ok(view(repo.findSubscription(id).orElseThrow()));
    }

    @PostMapping("/{id}/pause")
    public ApiResponse<Map<String, Object>> pause(@PathVariable String id) {
        engine.pause(id);
        audit.record("PAUSE", "SUBSCRIPTION", id, null, null);
        return ApiResponse.ok(view(repo.findSubscription(id).orElseThrow()));
    }

    @PostMapping("/{id}/resume")
    public ApiResponse<Map<String, Object>> resume(@PathVariable String id) {
        engine.resume(id);
        audit.record("RESUME", "SUBSCRIPTION", id, null, null);
        return ApiResponse.ok(view(repo.findSubscription(id).orElseThrow()));
    }

    @GetMapping("/active")
    public ApiResponse<List<Map<String, Object>>> active() {
        return ApiResponse.ok(engine.activeStatus());
    }

    Map<String, Object> view(SubscriptionDef d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", d.getId());
        m.put("serverId", d.getServerId());
        m.put("topicId", d.getTopicId());
        m.put("topic", d.getTopic());
        m.put("group", d.getGroup());
        m.put("state", d.getState());
        m.put("qos", d.getQos());
        m.put("templateIds", d.getTemplateIds());
        m.put("autoMatch", d.isAutoMatch());
        m.put("dlqEnabled", d.isDlqEnabled());
        m.put("persistent", d.isPersistent());
        m.put("offsetStrategy", d.getOffsetStrategy());
        m.put("createdAt", d.getCreatedAt());
        return m;
    }
}
