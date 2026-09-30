package io.mngr.core.template;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.TemplateStatus;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.TemplateDef;
import io.mngr.core.spi.ConfigRepository;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** 模板生命周期编排：保存即新版本、破坏性变更门禁、引用检查、指纹缓存失效。 */
@RequiredArgsConstructor
public class TemplateService {

    private final ConfigRepository repo;
    private final TemplateMatcher matcher;

    public TemplateDef create(TemplateDef t) {
        if (t.getName() == null || t.getName().isBlank()) {
            throw new MngrException(ErrorCode.VALIDATION, "模板名称必填");
        }
        if (t.getId() == null || t.getId().isBlank()) {
            t.setId("tpl-" + java.util.UUID.randomUUID().toString().substring(0, 8));
        }
        if (repo.findTemplate(t.getId()).isPresent()) {
            throw new MngrException(ErrorCode.VALIDATION, "模板 id 已存在，请走更新");
        }
        t.setVersion(1);
        t.setCreatedAt(Instant.now());
        t.setSource(t.getSource() == null ? "MANUAL" : t.getSource());
        repo.saveTemplate(t);
        matcher.invalidate(t.getId());
        return t;
    }

    /** 更新 = 新版本；破坏性变更需 force=true（FR-6.4 / 6002）。 */
    public TemplateDef update(TemplateDef t, boolean force) {
        Optional<TemplateDef> cur = repo.findTemplate(t.getId());
        if (cur.isEmpty()) throw new MngrException(ErrorCode.TEMPLATE_NOT_FOUND);
        VersionGuard.Diff d = VersionGuard.diff(cur.get().getFields(), t.getFields());
        if (d.breaking() && !force) {
            throw new MngrException(ErrorCode.BREAKING_CHANGE,
                    "不兼容变更: " + String.join("; ", d.changes()), d.changes());
        }
        TemplateDef next = t.toBuilder()
                .version(cur.get().getVersion() + 1)
                .createdAt(cur.get().getCreatedAt())
                .status(t.getStatus() == null ? cur.get().getStatus() : t.getStatus())
                .source(t.getSource() == null ? cur.get().getSource() : t.getSource())
                .build();
        repo.saveTemplate(next);
        matcher.invalidate(next.getId());
        return next;
    }

    public Optional<TemplateDef> get(String id) { return repo.findTemplate(id); }

    public Optional<TemplateDef> get(String id, int version) { return repo.findTemplate(id, version); }

    public List<TemplateDef> versions(String id) { return repo.listTemplateVersions(id); }

    public void setStatus(String id, TemplateStatus status) {
        TemplateDef cur = repo.findTemplate(id).orElseThrow(() -> new MngrException(ErrorCode.TEMPLATE_NOT_FOUND));
        repo.updateTemplateStatus(id, cur.getVersion(), status);
        matcher.invalidate(id);
    }

    public void delete(String id, boolean force) {
        TemplateDef cur = repo.findTemplate(id).orElseThrow(() -> new MngrException(ErrorCode.TEMPLATE_NOT_FOUND));
        if (!force && isReferenced(id)) {
            throw new MngrException(ErrorCode.TEMPLATE_IN_USE);
        }
        repo.deleteTemplate(id);
        matcher.invalidate(id);
        return;
    }

    public boolean isReferenced(String id) {
        for (var t : repo.listTopics(null, null, 0, 10000)) {
            if (id.equals(t.getTemplateId())) return true;
        }
        for (var s : repo.listSubscriptions(null)) {
            if (s.getTemplateIds() != null && s.getTemplateIds().contains(id)) return true;
        }
        return false;
    }

    /** 供匹配/校验使用的启用模板（其他状态返回 DRAFT/DISABLED 由调用方决定）。 */
    public Optional<TemplateDef> enabled(String id) {
        return repo.findTemplate(id).filter(t -> t.getStatus() == TemplateStatus.ENABLED);
    }
}
