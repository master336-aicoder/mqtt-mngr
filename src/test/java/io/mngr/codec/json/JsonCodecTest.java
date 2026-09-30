package io.mngr.codec.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.FieldDef;
import io.mngr.core.model.TypedPayload;
import io.mngr.store.StoreJson;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonCodecTest {

    private final JsonCodec codec = new JsonCodec();

    private static FieldDef f(String name, String type, boolean required) {
        return FieldDef.builder().name(name).type(type).required(required).build();
    }

    @Test
    void decodeInvalidJsonThrows5001() {
        MngrException e = assertThrows(MngrException.class, () -> codec.decode("{oops".getBytes()));
        assertEquals(ErrorCode.DECODE_FAILED.code(), e.getCode());
    }

    @Test
    void applyCoercesStringNumberAndFlagsEnum() {
        JsonNode raw = StoreJson.mapper().valueToTree(
                java.util.Map.of("deviceId", "a1", "temp", "36.50", "alarm", "BOGUS"));
        List<FieldDef> fields = List.of(
                f("deviceId", "string", true),
                f("temp", "number", true),
                FieldDef.builder().name("alarm").type("string").required(false)
                        .enumValues(List.of("NONE", "WARN", "CRIT")).build());
        TypedPayload tp = codec.apply(raw, fields);
        assertEquals(36.5, tp.getValues().get("temp").asDouble(), 1e-9);
        assertEquals(List.of("alarm 取值不在枚举内: BOGUS"), tp.getViolations());
    }

    @Test
    void applyMissingRequiredAndDefaults() {
        JsonNode raw = StoreJson.mapper().createObjectNode();
        List<FieldDef> fields = List.of(
                f("id", "string", true),
                FieldDef.builder().name("qos").type("integer").required(false)
                        .defaultValue(1).build());
        TypedPayload tp = codec.apply(raw, fields);
        assertEquals(List.of("缺少必填字段 id"), tp.getViolations());
        assertEquals(1, tp.getValues().get("qos").asInt());
    }

    @Test
    void applyCollectsUnknownFieldsIgnoringUnderscore() {
        JsonNode raw = StoreJson.mapper().valueToTree(
                java.util.Map.of("a", 1, "extra", 2, "_meta", 3));
        TypedPayload tp = codec.apply(raw, List.of(f("a", "integer", true)));
        assertEquals(List.of("extra"), tp.getUnknownFields());
    }

    @Test
    void applyNestedArrayChildren() throws Exception {
        JsonNode raw = StoreJson.mapper().readValue("""
                {"items":[{"sku":"S1","qty":"5"},{"sku":"S2","qty":"x"}]}""", JsonNode.class);
        FieldDef items = FieldDef.builder().name("items").type("array").required(true)
                .children(List.of(f("sku", "string", true), f("qty", "integer", true))).build();
        TypedPayload tp = codec.apply(raw, List.of(items));
        assertEquals(5, tp.getValues().get("items").get(0).get("qty").asInt());
        assertTrue(tp.getViolations().stream().anyMatch(v -> v.contains("items[].qty")));
    }

    @Test
    void topNonObjectIsViolation() throws Exception {
        JsonNode raw = StoreJson.mapper().readValue("[1,2]", JsonNode.class);
        TypedPayload tp = codec.apply(raw, List.of(f("a", "string", false)));
        assertEquals(List.of("payload 顶层不是 JSON 对象"), tp.getViolations());
    }

    @Test
    void flattenAndTemplatePathsAreIsomorphic() throws Exception {
        JsonNode raw = StoreJson.mapper().readValue("""
                {"a":1,"b":{"c":"x"},"d":[true]}""", JsonNode.class);
        List<String> msg = JsonCodec.flattenPaths(raw);
        assertTrue(msg.contains("a:integer"));
        assertTrue(msg.contains("b.c:string"));
        assertTrue(msg.contains("d[]:boolean"));
    }
}
