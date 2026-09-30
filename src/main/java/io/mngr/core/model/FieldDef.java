package io.mngr.core.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/** 模板字段定义（FR-6.1），type: string|number|integer|boolean|object|array|any。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder(toBuilder = true)
public class FieldDef {
    private String name;
    private String type;
    @Builder.Default
    private boolean required = false;
    private String comment;
    private Object example;
    private Object defaultValue;
    private List<String> enumValues;
    private List<FieldDef> children;   // object/array 嵌套，深度≤5

    public boolean isContainer() {
        return "object".equals(type) || "array".equals(type);
    }
}
