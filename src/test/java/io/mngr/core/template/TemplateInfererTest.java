package io.mngr.core.template;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.model.FieldDef;
import io.mngr.store.StoreJson;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TemplateInfererTest {

    private static JsonNode n(String json) throws Exception {
        return StoreJson.mapper().readTree(json);
    }

    private static Optional<FieldDef> find(List<FieldDef> fs, String name) {
        return fs.stream().filter(f -> f.getName().equals(name)).findFirst();
    }

    @Test
    void mergesSamplesAndDetectsTypes() throws Exception {
        List<FieldDef> out = TemplateInferer.infer(List.of(
                n("{\"id\":\"a\",\"v\":1,\"ok\":true}"),
                n("{\"id\":\"b\",\"v\":2.5,\"extra\":{\"k\":1}}")));
        assertEquals("string", find(out, "id").orElseThrow().getType());
        assertEquals("number", find(out, "v").orElseThrow().getType()); // integer+number → number
        assertEquals("boolean", find(out, "ok").orElseThrow().getType());
        assertEquals("object", find(out, "extra").orElseThrow().getType());
        assertNotNull(find(out, "extra").orElseThrow().getChildren());
        assertEquals("integer", find(find(out, "extra").orElseThrow().getChildren(), "k").orElseThrow().getType());
    }

    @Test
    void enumCandidatesForLowCardinalityStrings() throws Exception {
        List<FieldDef> out = TemplateInferer.infer(List.of(
                n("{\"s\":\"ON\"}"), n("{\"s\":\"OFF\"}"), n("{\"s\":\"ON\"}"), n("{\"s\":\"OFF\"}")));
        FieldDef s = find(out, "s").orElseThrow();
        assertEquals(List.of("ON", "OFF"), s.getEnumValues());
    }

    @Test
    void dateStringsGetCommentNotEnum() throws Exception {
        List<FieldDef> out = TemplateInferer.infer(List.of(
                n("{\"t\":\"2026-09-30T10:00:00Z\"}"), n("{\"t\":\"2026-09-30T10:00:01Z\"}")));
        FieldDef t = find(out, "t").orElseThrow();
        assertNull(t.getEnumValues());
        assertEquals("疑似日期/时间字符串", t.getComment());
    }

    @Test
    void arrayElementsBecomeChildren() throws Exception {
        List<FieldDef> out = TemplateInferer.infer(List.of(
                n("{\"items\":[{\"sku\":\"S1\",\"qty\":2},{\"sku\":\"S2\",\"qty\":7}]}")));
        FieldDef items = find(out, "items").orElseThrow();
        assertEquals("array", items.getType());
        assertEquals(2, items.getChildren().size());
    }

    @Test
    void mixedTypesBecomeAny() throws Exception {
        List<FieldDef> out = TemplateInferer.infer(List.of(n("{\"x\":1}"), n("{\"x\":\"s\"}")));
        assertEquals("any", find(out, "x").orElseThrow().getType());
    }
}
