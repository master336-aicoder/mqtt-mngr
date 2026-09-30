package io.mngr.core.template;

import io.mngr.codec.json.JsonCodec;
import io.mngr.core.model.FieldDef;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/** 结构指纹：字段全路径+类型桶 hash（§7.3）。 */
public final class StructureFingerprint {

    private StructureFingerprint() {}

    public static String ofMessage(List<String> flatPaths) {
        return hash(String.join("|", flatPaths));
    }

    public static String ofTemplate(List<FieldDef> fields) {
        return hash(String.join("|", JsonCodec.templatePaths(fields)));
    }

    static String hash(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 12; i++) sb.append(String.format("%02x", d[i]));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
