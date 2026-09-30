package io.mngr.core.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/** 发布结果（FR-3.3）。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
public class PublishResult {
    private boolean success;
    private String messageId;
    private String serverId;
    private String topic;
    /** 协议细节：Kafka partition/offset，MQTT packetId/reasonCode。 */
    private Map<String, Object> protocolMeta;
    private Integer errorCode;
    private String errorMessage;
}
