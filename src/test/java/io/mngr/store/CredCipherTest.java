package io.mngr.store;

import io.mngr.config.MngrProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CredCipherTest {

    private static MngrProperties props(String key) {
        MngrProperties p = new MngrProperties();
        p.setMasterKey(key);
        return p;
    }

    @Test
    void roundTrip() {
        CredCipher cipher = new CredCipher(props("test-key"));
        Map<String, String> creds = Map.of("username", "admin", "password", "s3cr3t#中文");
        byte[] enc = cipher.encrypt(creds);
        assertEquals("admin", cipher.decrypt(enc).get("username"));
        assertEquals("s3cr3t#中文", cipher.decrypt(enc).get("password"));
    }

    @Test
    void ciphertextNonDeterministic() {
        CredCipher cipher = new CredCipher(props("test-key"));
        Map<String, String> creds = Map.of("password", "same");
        byte[] a = cipher.encrypt(creds);
        byte[] b = cipher.encrypt(creds);
        assertFalse(java.util.Arrays.equals(a, b), "IV 应随机");
        assertEquals(cipher.decrypt(a).get("password"), cipher.decrypt(b).get("password"));
    }

    @Test
    void tamperedCiphertextRejected() {
        CredCipher cipher = new CredCipher(props("test-key"));
        byte[] enc = cipher.encrypt(Map.of("password", "v"));
        enc[enc.length - 1] ^= 0x55; // 破坏 GCM tag
        assertThrows(Exception.class, () -> cipher.decrypt(enc));
    }

    @Test
    void wrongKeyCannotDecrypt() {
        byte[] enc = new CredCipher(props("key-A")).encrypt(Map.of("password", "v"));
        CredCipher other = new CredCipher(props("key-B"));
        assertThrows(Exception.class, () -> other.decrypt(enc));
    }

    @Test
    void base64Helpers() {
        CredCipher cipher = new CredCipher(props("k"));
        byte[] enc = cipher.encrypt(Map.of("a", "b"));
        String b64 = CredCipher.toBase64(enc);
        assertArrayEquals(enc, CredCipher.fromBase64(b64));
    }
}
