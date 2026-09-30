package io.mngr.store;

import io.mngr.config.MngrProperties;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

/** 凭据 AES-256-GCM 加解密：密钥 = SHA-256(masterKey)，IV 随机 12B 前置。 */
public final class CredCipher {

    private static final int IV_LEN = 12;
    private static final int TAG_BITS = 128;
    private final SecretKeySpec key;

    public CredCipher(MngrProperties props) {
        try {
            byte[] k = MessageDigest.getInstance("SHA-256")
                    .digest(props.getMasterKey().getBytes(StandardCharsets.UTF_8));
            this.key = new SecretKeySpec(k, "AES");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public byte[] encrypt(Map<String, String> creds) {
        if (creds == null || creds.isEmpty()) return null;
        try {
            String json = StoreJson.mapper().writeValueAsString(creds);
            byte[] iv = new byte[IV_LEN];
            new SecureRandom().nextBytes(iv);
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ct = c.doFinal(json.getBytes(StandardCharsets.UTF_8));
            byte[] out = new byte[iv.length + ct.length];
            System.arraycopy(iv, 0, out, 0, iv.length);
            System.arraycopy(ct, 0, out, iv.length, ct.length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("credential encrypt failed", e);
        }
    }

    public Map<String, String> decrypt(byte[] enc) {
        if (enc == null || enc.length <= IV_LEN) return Map.of();
        try {
            byte[] iv = new byte[IV_LEN];
            System.arraycopy(enc, 0, iv, 0, IV_LEN);
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] json = c.doFinal(enc, IV_LEN, enc.length - IV_LEN);
            return StoreJson.mapper().readValue(json,
                    StoreJson.mapper().getTypeFactory().constructMapType(
                            java.util.HashMap.class, String.class, String.class));
        } catch (Exception e) {
            throw new IllegalStateException("credential decrypt failed (wrong master key?)", e);
        }
    }

    public static String toBase64(byte[] enc) {
        return enc == null ? null : Base64.getEncoder().encodeToString(enc);
    }

    public static byte[] fromBase64(String s) {
        return (s == null || s.isBlank()) ? null : Base64.getDecoder().decode(s);
    }
}
