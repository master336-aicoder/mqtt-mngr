package io.mngr.core.topic;

/** MQTT 通配匹配（+ 单层，# 多层）。Kafka 精确。 */
public final class WildcardMatcher {

    private WildcardMatcher() {}

    public static boolean matches(String filter, String topic) {
        if (filter.equals(topic)) return true;
        if (!filter.contains("+") && !filter.contains("#")) return false;
        // $SYS 主题：不含前导 $ 的 filter 中 # / + 首层不匹配 $开头主题
        if (topic.startsWith("$") && (filter.startsWith("#") || filter.startsWith("+"))) return false;
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        for (int i = 0; i < f.length; i++) {
            if (f[i].equals("#")) return true;
            if (i >= t.length) return false;
            if (f[i].equals("+")) {
                if (t[i].isEmpty()) return false;
                continue;
            }
            if (!f[i].equals(t[i])) return false;
        }
        return f.length == t.length;
    }

    /** 发布 topic 名是否命中登记模式（用于 TopicConfig 反查）。 */
    public static String resolveProtocolAgnostic(String filter, String topic) {
        return matches(filter, topic) ? filter : null;
    }
}
