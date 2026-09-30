package io.mngr.core.model;

import io.mngr.core.enums.TemplateStatus;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

/** 消息模板（FR-6），复合键 id+version。 */
@Data
@Builder(toBuilder = true)
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public class TemplateDef {
    private String id;
    @Builder.Default
    private int version = 1;
    private String name;
    private String description;
    @Builder.Default
    private TemplateStatus status = TemplateStatus.ENABLED;
    private List<FieldDef> fields;
    private String source;      // MANUAL | INFERRED | SCHEMA_IMPORT
    private String owner;
    private String tags;
    private Instant createdAt;
    private Instant updatedAt;
}
