package io.mngr.web;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.codec.json.JsonCodec;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.TemplateStatus;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.FieldDef;
import io.mngr.core.model.Page;
import io.mngr.core.model.TemplateDef;
import io.mngr.core.model.TypedPayload;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.template.TemplateInferer;
import io.mngr.core.template.TemplateMatcher;
import io.mngr.core.template.TemplateService;
import io.mngr.store.StoreJson;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1/templates")
@RequiredArgsConstructor
public class TemplateController {

    private final ConfigRepository repo;
    private final TemplateService service;
    private final TemplateMatcher matcher;
    private final JsonCodec codec;
    private final AuditHelper audit;

    @Data
    public static class InferReq {
        private List<String> samples;   // JSON 字符串样例
        private String name;
        private boolean save = false;   // true 时保存为 DRAFT
    }

    @Data
    public static class MatchTestReq {
        private String payload;
        private List<String> templateIds;
    }

    @GetMapping
    public ApiResponse<Page<Map<String, Object>>> list(@RequestParam(required = false) String keyword,
                                                       @RequestParam(defaultValue = "0") int page,
                                                       @RequestParam(defaultValue = "20") int size) {
        List<TemplateDef> items = repo.listTemplates(keyword, page, size);
        long total = repo.countTemplates(keyword);
        return ApiResponse.ok(new Page<>(items.stream().map(this::view).toList(), total));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable String id) {
        return ApiResponse.ok(view(service.get(id)
                .orElseThrow(() -> new MngrException(ErrorCode.TEMPLATE_NOT_FOUND))));
    }

    @GetMapping("/{id}/versions")
    public ApiResponse<List<Map<String, Object>>> versions(@PathVariable String id) {
        return ApiResponse.ok(service.versions(id).stream().map(this::view).toList());
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> create(@RequestBody TemplateDef in) {
        TemplateDef saved = service.create(in);
        audit.record("CREATE", "TEMPLATE", saved.getId(), null, saved);
        return ApiResponse.ok(view(saved));
    }

    @PutMapping("/{id}")
    public ApiResponse<Map<String, Object>> update(@PathVariable String id,
                                                   @RequestParam(defaultValue = "false") boolean force,
                                                   @RequestBody TemplateDef in) {
        TemplateDef before = service.get(id).orElseThrow(() -> new MngrException(ErrorCode.TEMPLATE_NOT_FOUND));
        in.setId(id);
        TemplateDef saved = service.update(in, force);
        audit.record("UPDATE", "TEMPLATE", id, before, saved);
        return ApiResponse.ok(view(saved));
    }

    @PostMapping("/{id}/status")
    public ApiResponse<Map<String, Object>> setStatus(@PathVariable String id, @RequestBody StatusReq req) {
        service.setStatus(id, TemplateStatus.valueOf(req.getStatus().toUpperCase()));
        audit.record("UPDATE", "TEMPLATE", id, null, Map.of("status", req.getStatus()));
        return ApiResponse.ok(view(service.get(id).orElseThrow()));
    }

    @Data
    public static class StatusReq {
        private String status;
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable String id,
                                    @RequestParam(defaultValue = "false") boolean force) {
        service.delete(id, force);
        audit.record("DELETE", "TEMPLATE", id, null, null);
        return ApiResponse.ok(null);
    }

    /** 样例反推（FR-6.5）→ 字段树（save=true 落 DRAFT）。 */
    @PostMapping("/infer")
    public ApiResponse<Map<String, Object>> infer(@RequestBody InferReq req) {
        if (req.getSamples() == null || req.getSamples().isEmpty()) {
            throw new MngrException(ErrorCode.VALIDATION, "samples 必填");
        }
        List<JsonNode> nodes = new ArrayList<>();
        for (String s : req.getSamples()) {
            try {
                nodes.add(StoreJson.mapper().readTree(s));
            } catch (Exception e) {
                throw new MngrException(ErrorCode.DECODE_FAILED, "样例非法 JSON: " + e.getMessage());
            }
        }
        List<FieldDef> fields = TemplateInferer.infer(nodes);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("fields", fields);
        if (req.isSave()) {
            TemplateDef t = TemplateDef.builder()
                    .id("tpl-" + UUID.randomUUID().toString().substring(0, 8))
                    .name(req.getName() == null ? "inferred-" + System.currentTimeMillis() : req.getName())
                    .description("由 " + nodes.size() + " 条样例反推")
                    .status(TemplateStatus.DRAFT)
                    .source("INFERRED")
                    .fields(fields)
                    .build();
            out.put("template", view(service.create(t)));
            audit.record("CREATE", "TEMPLATE", t.getId(), null, t);
        }
        return ApiResponse.ok(out);
    }

    /** 匹配测试：payload → 匹配链结果 + 模板校验明细。 */
    @PostMapping("/match-test")
    public ApiResponse<Map<String, Object>> matchTest(@RequestBody MatchTestReq req) {
        JsonNode node;
        try {
            node = StoreJson.mapper().readTree(req.getPayload());
        } catch (Exception e) {
            throw new MngrException(ErrorCode.DECODE_FAILED, "payload 非法 JSON");
        }
        TemplateMatcher.MatchResult mr = matcher.match(null, req.getTemplateIds(), node);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("match", mr);
        if (mr.getTemplateId() != null) {
            repo.findTemplate(mr.getTemplateId(), mr.getVersion() == null ? 0 : mr.getVersion())
                    .ifPresent(t -> {
                        TypedPayload tp = codec.apply(node, t.getFields());
                        out.put("violations", tp.getViolations());
                        out.put("unknownFields", tp.getUnknownFields());
                        out.put("typedJson", tp.getValues().toString());
                    });
        }
        return ApiResponse.ok(out);
    }

    Map<String, Object> view(TemplateDef t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", t.getId());
        m.put("version", t.getVersion());
        m.put("name", t.getName());
        m.put("description", t.getDescription());
        m.put("status", t.getStatus().name());
        m.put("fields", t.getFields());
        m.put("source", t.getSource());
        m.put("owner", t.getOwner());
        m.put("tags", t.getTags());
        m.put("createdAt", t.getCreatedAt());
        m.put("updatedAt", t.getUpdatedAt());
        m.put("referenced", service.isReferenced(t.getId()));
        return m;
    }
}
