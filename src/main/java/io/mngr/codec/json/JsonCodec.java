package io.mngr.codec.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.FieldDef;
import io.mngr.core.model.TypedPayload;
import io.mngr.core.spi.MessageCodec;
import io.mngr.store.StoreJson;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** JSON 编解码 + 模板感知转换（FR-5 / FR-6.4）。由 CoreConfig 装配。 */
public class JsonCodec implements MessageCodec {

    @Override
    public String name() { return "json"; }

    @Override
    public JsonNode decode(byte[] payload) {
        try {
            JsonNode n = StoreJson.mapper().readTree(payload);
            if (n == null) throw new MngrException(ErrorCode.DECODE_FAILED, "空载荷");
            return n;
        } catch (MngrException e) {
            throw e;
        } catch (Exception e) {
            throw new MngrException(ErrorCode.DECODE_FAILED, "JSON 解析失败: " + e.getMessage(), e);
        }
    }

    @Override
    public byte[] encode(JsonNode node) {
        try {
            return StoreJson.mapper().writeValueAsString(node).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new MngrException(ErrorCode.VALIDATION, "payload 序列化失败", e);
        }
    }

    @Override
    public List<String> validateAgainstTemplate(JsonNode node, List<FieldDef> fields) {
        return apply(node, fields).getViolations();
    }

    /** 模板驱动：类型矫正、默认值填充、必填校验、未知字段收集。 */
    public TypedPayload apply(JsonNode raw, List<FieldDef> fields) {
        List<String> violations = new ArrayList<>();
        List<String> unknown = new ArrayList<>();
        JsonNode out = raw == null ? StoreJson.mapper().createObjectNode() : raw.deepCopy();
        if (!out.isObject()) {
            // 顶层非对象：按单字段 "__root" 处理，直接视为不匹配
            violations.add("payload 顶层不是 JSON 对象");
            return new TypedPayload(out, raw, unknown, violations);
        }
        walk((ObjectNode) out, fields, "", violations, unknown, true);
        return new TypedPayload(out, raw, unknown, violations);
    }

    private void walk(ObjectNode obj, List<FieldDef> defs, String prefix,
                      List<String> violations, List<String> unknown, boolean strictExtra) {
        for (FieldDef d : defs) {
            String path = prefix.isEmpty() ? d.getName() : prefix + "." + d.getName();
            JsonNode v = obj.get(d.getName());
            if (v == null || v.isNull()) {
                if (d.getDefaultValue() != null) {
                    obj.set(d.getName(), StoreJson.mapper().valueToTree(d.getDefaultValue()));
                } else if (d.isRequired()) {
                    violations.add("缺少必填字段 " + path);
                }
                continue;
            }
            JsonNode coerced = coerce(v, d, path, violations);
            if (coerced != v) obj.set(d.getName(), coerced);
            if (d.isContainer() && d.getChildren() != null && !d.getChildren().isEmpty()) {
                if (coerced.isObject()) {
                    walk((ObjectNode) coerced, d.getChildren(), path, violations, unknown, strictExtra);
                } else if (coerced.isArray()) {
                    for (JsonNode el : coerced) {
                        if (el.isObject()) walk((ObjectNode) el, d.getChildren(), path + "[]", violations, unknown, strictExtra);
                    }
                }
            }
        }
        if (strictExtra) {
            java.util.Set<String> declared = new java.util.HashSet<>();
            defs.forEach(d -> declared.add(d.getName()));
            obj.fieldNames().forEachRemaining(n -> {
                if (!declared.contains(n) && !n.startsWith("_")) {
                    unknown.add(prefix.isEmpty() ? n : prefix + "." + n);
                }
            });
        }
    }

    private JsonNode coerce(JsonNode v, FieldDef d, String path, List<String> violations) {
        String t = d.getType() == null ? "any" : d.getType();
        JsonNode result = v;
        switch (t) {
            case "integer" -> {
                if (v.isInt() || v.isLong()) return v;
                if (v.isNumber()) {
                    result = StoreJson.mapper().getNodeFactory().numberNode(v.asLong());
                } else if (v.isTextual()) {
                    try { result = StoreJson.mapper().getNodeFactory().numberNode(Long.parseLong(v.asText().trim())); }
                    catch (NumberFormatException e) { violations.add(path + " 无法转为 integer: " + v.asText()); }
                } else {
                    violations.add(path + " 类型不符（期望 integer）");
                }
            }
            case "number" -> {
                if (v.isNumber()) return v;
                if (v.isTextual()) {
                    try { result = StoreJson.mapper().getNodeFactory().numberNode(Double.parseDouble(v.asText().trim())); }
                    catch (NumberFormatException e) { violations.add(path + " 无法转为 number: " + v.asText()); }
                } else {
                    violations.add(path + " 类型不符（期望 number）");
                }
            }
            case "boolean" -> {
                if (v.isBoolean()) return v;
                if (v.isTextual() && (v.asText().equalsIgnoreCase("true") || v.asText().equalsIgnoreCase("false"))) {
                    result = StoreJson.mapper().getNodeFactory().booleanNode(Boolean.parseBoolean(v.asText()));
                } else {
                    violations.add(path + " 类型不符（期望 boolean）");
                }
            }
            case "string" -> {
                if (!v.isTextual()) result = StoreJson.mapper().getNodeFactory().textNode(v.asText());
            }
            case "object" -> {
                if (!v.isObject()) violations.add(path + " 类型不符（期望 object）");
            }
            case "array" -> {
                if (!v.isArray()) violations.add(path + " 类型不符（期望 array）");
            }
            default -> { }
        }
        if (d.getEnumValues() != null && !d.getEnumValues().isEmpty()
                && result.isTextual() && !d.getEnumValues().contains(result.asText())) {
            violations.add(path + " 取值不在枚举内: " + result.asText());
        }
        return result;
    }

    /** 打平 JSON → 路径:类型桶 列表（匹配用）。 */
    public static List<String> flattenPaths(JsonNode node) {
        List<String> out = new ArrayList<>();
        flatten(node, "", out, 0);
        java.util.Collections.sort(out);
        return out;
    }

    static void flatten(JsonNode n, String prefix, List<String> out, int depth) {
        if (depth > 6) return;
        if (n.isObject()) {
            n.fields().forEachRemaining(e -> {
                String p = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
                JsonNode c = e.getValue();
                if (c.isObject() && !c.isEmpty()) flatten(c, p, out, depth + 1);
                else if (c.isArray()) {
                    String ap = p + "[]";
                    out.add(ap + ":" + bucket(c.isEmpty() ? null : c.get(0)));
                    for (JsonNode el : c) {
                        if (el.isObject()) { flatten(el, ap, out, depth + 1); break; }
                    }
                } else out.add(p + ":" + bucket(c));
            });
        }
    }

    static String bucket(JsonNode n) {
        if (n == null) return "any";
        if (n.isInt() || n.isLong()) return "integer";
        if (n.isNumber()) return "number";
        if (n.isBoolean()) return "boolean";
        if (n.isArray()) return "array";
        if (n.isObject()) return "object";
        return "string";
    }

    /** FieldDef 树 → 路径:类型桶 列表（与 flattenPaths 同构，供打分）。 */
    public static List<String> templatePaths(List<FieldDef> defs) {
        List<String> out = new ArrayList<>();
        walkPaths(defs, "", out, 0);
        java.util.Collections.sort(out);
        return out;
    }

    static void walkPaths(List<FieldDef> defs, String prefix, List<String> out, int depth) {
        if (depth > 6 || defs == null) return;
        for (FieldDef d : defs) {
            String p = prefix.isEmpty() ? d.getName() : prefix + "." + d.getName();
            String t = d.getType() == null ? "any" : d.getType();
            out.add(p + ":" + t);
            if (d.getChildren() != null && !d.getChildren().isEmpty()) {
                if ("array".equals(t)) walkPaths(d.getChildren(), p + "[]", out, depth + 1);
                else walkPaths(d.getChildren(), p, out, depth + 1);
            }
        }
    }
}
