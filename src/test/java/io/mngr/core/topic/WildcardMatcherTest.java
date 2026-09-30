package io.mngr.core.topic;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WildcardMatcherTest {

    @Test
    void exactMatch() {
        assertTrue(WildcardMatcher.matches("device/a1/event", "device/a1/event"));
        assertFalse(WildcardMatcher.matches("device/a1/event", "device/a2/event"));
    }

    @Test
    void singleLevelPlus() {
        assertTrue(WildcardMatcher.matches("device/+/event", "device/a1/event"));
        assertFalse(WildcardMatcher.matches("device/+/event", "device/a1/b2/event"));
        assertFalse(WildcardMatcher.matches("device/+/event", "device//event"));
    }

    @Test
    void multiLevelHash() {
        assertTrue(WildcardMatcher.matches("device/#", "device/a1/event"));
        assertTrue(WildcardMatcher.matches("device/#", "device/a1"));
        assertFalse(WildcardMatcher.matches("device/#", "devices/a1"));
        assertTrue(WildcardMatcher.matches("#", "a/b/c"));
    }

    @Test
    void dollarSysProtection() {
        // 首层为通配的 filter 不匹配 $ 开头主题（MQTT 规范）
        assertFalse(WildcardMatcher.matches("#", "$SYS/broker/version"));
        assertFalse(WildcardMatcher.matches("+/x", "$SYS/x"));
        assertTrue(WildcardMatcher.matches("$SYS/#", "$SYS/broker/version"));
    }

    @Test
    void depthMismatch() {
        assertTrue(WildcardMatcher.matches("device/+", "device/a1"));
        assertFalse(WildcardMatcher.matches("device/+", "device/a1/event"));
        assertFalse(WildcardMatcher.matches("device/+/event", "device/a1"));
    }
}
