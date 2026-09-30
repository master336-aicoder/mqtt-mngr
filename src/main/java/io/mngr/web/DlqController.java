package io.mngr.web;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.DlqRecord;
import io.mngr.core.model.Page;
import io.mngr.core.model.PublishRequest;
import io.mngr.core.model.PublishResult;
import io.mngr.core.pub.PublisherEngine;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.store.DlqQuery;
import io.mngr.store.file.FileRuntimeRepository;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/v1/dlq")
@RequiredArgsConstructor
public class DlqController {

    private final RuntimeRepository runtime;
    private final PublisherEngine publisherEngine;
    private final AuditHelper audit;

    @Data
    public static class IdsReq {
        private List<Long> ids;
    }

    @GetMapping
    public ApiResponse<Page<Map<String, Object>>> query(@RequestParam(required = false) String serverId,
                                                        @RequestParam(required = false) String topic,
                                                        @RequestParam(required = false) String subscriptionId,
                                                        @RequestParam(required = false) Integer errCode,
                                                        @RequestParam(required = false) Instant from,
                                                        @RequestParam(required = false) Instant to,
                                                        @RequestParam(defaultValue = "0") int page,
                                                        @RequestParam(defaultValue = "20") int size) {
        if (!runtime.capabilities().contains(RuntimeRepository.RuntimeCapability.DLQ_QUERY)) {
            throw new MngrException(ErrorCode.FEATURE_DISABLED,
                    "当前存储模式不支持 DLQ 条件分页查询，请使用 /api/v1/dlq/tail");
        }
        DlqQuery q = DlqQuery.builder().serverId(serverId).topic(topic).subscriptionId(subscriptionId)
                .errCode(errCode).from(from).to(to).page(page).size(size).build();
        Page<DlqRecord> p = runtime.queryDlq(q);
        return ApiResponse.ok(new Page<>(p.getItems().stream().map(this::view).toList(), p.getTotal()));
    }

    /** 尾部扫描（两种模式均可用）。 */
    @GetMapping("/tail")
    public ApiResponse<Page<Map<String, Object>>> tail(@RequestParam(defaultValue = "50") int limit) {
        Page<DlqRecord> p;
        if (runtime instanceof FileRuntimeRepository f) {
            p = f.tailDlq(limit);
        } else {
            p = runtime.queryDlq(DlqQuery.builder().page(0).size(limit).build());
        }
        return ApiResponse.ok(new Page<>(p.getItems().stream().map(this::view).toList(), p.getTotal()));
    }

    @PostMapping("/replay")
    public ApiResponse<Map<String, Object>> replay(@RequestBody IdsReq req) {
        if (!runtime.capabilities().contains(RuntimeRepository.RuntimeCapability.DLQ_REPLAY)) {
            throw new MngrException(ErrorCode.FEATURE_DISABLED, "当前存储模式不支持 DLQ 重放");
        }
        List<DlqRecord> records = runtime.findDlqByIds(req.getIds());
        int ok = 0;
        List<Map<String, Object>> failed = new ArrayList<>();
        List<Long> replayed = new ArrayList<>();
        for (DlqRecord r : records) {
            try {
                PublishRequest pr = new PublishRequest();
                pr.setServerId(r.getServerId());
                pr.setTopic(r.getTopic());
                pr.setPayload(r.getPayload());
                Map<String, String> hs = new HashMap<>(r.getHeaders() == null ? Map.of() : r.getHeaders());
                hs.remove("x-msg-id");
                pr.setHeaders(hs);
                PublishResult res = publisherEngine.publish(pr);
                if (res.isSuccess()) {
                    ok++;
                    replayed.add(r.getId());
                } else {
                    failed.add(Map.of("id", r.getId(), "error", String.valueOf(res.getErrorMessage())));
                }
            } catch (Exception e) {
                failed.add(Map.of("id", r.getId(), "error", String.valueOf(e.getMessage())));
            }
        }
        if (!replayed.isEmpty()) {
            runtime.deleteDlq(replayed);
            audit.record("REPLAY", "DLQ", String.valueOf(replayed.size()), null, replayed);
        }
        return ApiResponse.ok(Map.of("requested", records.size(), "replayed", ok, "failed", failed));
    }

    @DeleteMapping
    public ApiResponse<Void> delete(@RequestBody IdsReq req) {
        runtime.deleteDlq(req.getIds());
        audit.record("DELETE", "DLQ", String.valueOf(req.getIds().size()), null, null);
        return ApiResponse.ok(null);
    }

    @GetMapping("/capabilities")
    public ApiResponse<Set<String>> capabilities() {
        Set<String> out = new TreeSet<>();
        runtime.capabilities().forEach(c -> out.add(c.name()));
        return ApiResponse.ok(out);
    }

    Map<String, Object> view(DlqRecord r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.getId());
        m.put("serverId", r.getServerId());
        m.put("topic", r.getTopic());
        m.put("subscriptionId", r.getSubscriptionId());
        m.put("templateId", r.getTemplateId());
        m.put("templateVersion", r.getTemplateVersion());
        m.put("headers", r.getHeaders());
        String payload = r.getPayload() == null ? "" : new String(r.getPayload(), java.nio.charset.StandardCharsets.UTF_8);
        if (payload.length() > 4000) payload = payload.substring(0, 4000) + "…";
        m.put("payloadPreview", payload);
        m.put("payloadBytes", r.getPayload() == null ? 0 : r.getPayload().length);
        m.put("errCode", r.getErrCode());
        m.put("errMsg", r.getErrMsg());
        m.put("origin", r.getOrigin());
        m.put("createdAt", r.getCreatedAt());
        return m;
    }
}
