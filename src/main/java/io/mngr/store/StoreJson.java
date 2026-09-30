package io.mngr.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** 存储层共享 Jackson mapper（Instant 以 epoch 数字序列化）。 */
public final class StoreJson {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private StoreJson() {}

    public static ObjectMapper mapper() { return MAPPER; }

    public static String write(Object o) {
        if (o == null) return null;
        try { return MAPPER.writeValueAsString(o); } catch (Exception e) { throw new IllegalStateException(e); }
    }

    public static <T> T read(String json, TypeReference<T> type) {
        if (json == null || json.isBlank()) return null;
        try { return MAPPER.readValue(json, type); } catch (Exception e) { throw new IllegalStateException(e); }
    }
}
