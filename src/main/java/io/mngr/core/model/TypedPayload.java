package io.mngr.core.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/** 模板感知解析产物：字段名→矫正值 + 原始树 + 未知字段清单。 */
@Data
@AllArgsConstructor
public class TypedPayload {
    private JsonNode values;                 // 矫正/默认值填充后的树
    private JsonNode raw;
    private List<String> unknownFields;
    private List<String> violations;         // 必填缺失等（非空则匹配失败）
}
