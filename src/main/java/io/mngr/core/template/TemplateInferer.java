package io.mngr.core.template;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.model.FieldDef;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

/** 样例反推模板（FR-6.5）：多样例合并，输出 DRAFT 字段树。 */
public final class TemplateInferer {

    private static final int MAX_DEPTH = 5;
    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
            DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.ISO_INSTANT, DateTimeFormatter.ISO_LOCAL_DATE_TIME);

    /** 字段名 → 观测值列表 + 子结构树。 */
    static class Acc {
        final LinkedHashMap<String, List<JsonNode>> values = new LinkedHashMap<>();
        final LinkedHashMap<String, Acc> children = new LinkedHashMap<>();
    }

    private TemplateInferer() {}

    public static List<FieldDef> infer(List<JsonNode> samples) {
        Acc root = new Acc();
        for (JsonNode s : samples) collect(s, root, 0);
        return build(root, 0);
    }

    private static void collect(JsonNode node, Acc acc, int depth) {
        if (depth > MAX_DEPTH || node == null || !node.isObject()) return;
        node.fields().forEachRemaining(e -> {
            String k = e.getKey();
            JsonNode v = e.getValue();
            acc.values.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
            if (v.isObject()) {
                collect(v, acc.children.computeIfAbsent(k, x -> new Acc()), depth + 1);
            } else if (v.isArray()) {
                Acc child = acc.children.computeIfAbsent(k, x -> new Acc());
                for (JsonNode el : v) {
                    if (el.isObject()) collect(el, child, depth + 1);
                }
            }
        });
    }

    private static List<FieldDef> build(Acc acc, int depth) {
        List<FieldDef> out = new ArrayList<>();
        for (Map.Entry<String, List<JsonNode>> e : acc.values.entrySet()) {
            String k = e.getKey();
            List<JsonNode> vals = e.getValue();
            String type = mergeType(vals);
            FieldDef.FieldDefBuilder b = FieldDef.builder().name(k).type(type).example(firstScalar(vals));
            if ("string".equals(type)) {
                Set<String> distinct = new LinkedHashSet<>();
                boolean allDate = true;
                for (JsonNode v : vals) {
                    if (!v.isTextual()) { allDate = false; break; }
                    distinct.add(v.asText());
                    if (!looksDate(v.asText())) allDate = false;
                }
                if (allDate && !distinct.isEmpty()) {
                    b.comment("疑似日期/时间字符串");
                } else if (!vals.isEmpty() && distinct.size() <= 6 && distinct.size() < vals.size()) {
                    b.enumValues(new ArrayList<>(distinct));
                    b.comment("枚举候选（来自样例去重）");
                }
            }
            Acc child = acc.children.get(k);
            if (child != null && depth < MAX_DEPTH && !child.values.isEmpty()
                    && ("object".equals(type) || "array".equals(type) || "any".equals(type))) {
                b.children(build(child, depth + 1));
            }
            out.add(b.build());
        }
        return out;
    }

    static String mergeType(List<JsonNode> vals) {
        Set<String> types = new TreeSet<>();
        for (JsonNode v : vals) {
            if (v.isNull()) continue;
            if (v.isInt() || v.isLong()) types.add("integer");
            else if (v.isNumber()) types.add("number");
            else if (v.isBoolean()) types.add("boolean");
            else if (v.isArray()) types.add("array");
            else if (v.isObject()) types.add("object");
            else types.add("string");
        }
        if (types.isEmpty()) return "any";
        if (types.size() == 1) return types.iterator().next();
        if (types.equals(Set.of("integer", "number"))) return "number";
        return "any";
    }

    static Object firstScalar(List<JsonNode> vals) {
        for (JsonNode v : vals) {
            if (v.isInt() || v.isLong()) return v.asLong();
            if (v.isNumber()) return v.asDouble();
            if (v.isBoolean()) return v.asBoolean();
            if (v.isTextual()) return v.asText();
        }
        return null;
    }

    static boolean looksDate(String s) {
        for (DateTimeFormatter f : DATE_FORMATS) {
            try { f.parse(s); return true; } catch (DateTimeParseException ignored) { }
        }
        return false;
    }
}
