package io.mngr.core.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

/** 订阅定义（FR-4）。 */
@Data
@Builder(toBuilder = true)
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriptionDef {
    private String id;
    private String serverId;
    private String topicId;          // 引用的 TopicConfig（可空=裸 topic）
    private String topic;            // 实际订阅名/模式
    private String group;            // Kafka groupId / MQTT $share group
    @Builder.Default
    private String state = "STOPPED"; // RUNNING|PAUSED|STOPPED
    private Integer qos;
    private List<String> templateIds; // 绑定模板候选集（BOUND）
    @Builder.Default
    private boolean autoMatch = true;
    @Builder.Default
    private int retryMax = 2;
    @Builder.Default
    private boolean dlqEnabled = true;
    @Builder.Default
    private boolean persistent = true; // 重启自动恢复
    @Builder.Default
    private String offsetStrategy = "LATEST"; // LATEST|EARLIEST|TIMESTAMP
    private Long offsetTimestamp;
    private Instant createdAt;
}
