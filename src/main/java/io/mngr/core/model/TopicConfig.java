package io.mngr.core.model;

import io.mngr.core.enums.Direction;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/** Topic 映射配置（FR-2.1）。 */
@Data
@Builder(toBuilder = true)
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public class TopicConfig {
    private String id;
    private String serverId;
    /** 精确名或通配模式（MQTT: device/#；Kafka 仅精确名）。 */
    private String namePattern;
    @Builder.Default
    private Direction direction = Direction.BOTH;
    @Builder.Default
    private String format = "json";
    private Integer qos;
    private String keyStrategy;      // Kafka: round-robin | key-hash
    private String templateId;       // 默认绑定模板（FR-6.3.1 BOUND）
    @Builder.Default
    private boolean autoMatch = true;
    private String tags;
    private String owner;
    private String remark;
    @Builder.Default
    private boolean deleted = false;
    private Instant updatedAt;
}
