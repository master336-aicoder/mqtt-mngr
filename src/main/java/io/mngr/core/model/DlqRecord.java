package io.mngr.core.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/** 死信记录（FR-3.4 / FR-4.5）。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
public class DlqRecord {
    private Long id;
    private String serverId;
    private String topic;
    private String subscriptionId;
    private String templateId;
    private Integer templateVersion;
    private Map<String, String> headers;
    private byte[] payload;
    private int errCode;
    private String errMsg;
    private Instant createdAt;
    /** 来源方向：PUB_FAIL | SUB_FAIL */
    private String origin;
}
