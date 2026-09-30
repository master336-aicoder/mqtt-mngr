package io.mngr.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/** 统一发布请求（FR-3.1）。payload 允许对象/字符串/数组。 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PublishRequest {
    private String serverId;
    private String topicId;     // 与 topic 二选一（引用 TopicConfig）
    private String topic;
    private Object payload;
    private String templateId;  // 显式模板（预校验 + x-template header）
    private Integer templateVersion;
    private String key;         // Kafka 分区键
    private Integer qos;        // MQTT
    private Boolean retain;     // MQTT
    private Long messageExpiryInterval; // MQTT5 M1x
    private String correlationId;       // MQTT5 M4x request/response
    @Deprecated
    private Map<String, String> headers;

    public Map<String, String> headersOrNew() {
        if (headers == null) headers = new HashMap<>();
        return headers;
    }
}
