package io.mngr.core.model;

import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/** 协议原始入件（Adapter → Core DispatchPipeline，不承担解析职责）。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
public class RawIncoming {
    private String serverId;
    private String subscriptionId;
    private String topic;
    private byte[] payload;
    @Builder.Default
    private Map<String, String> headers = new HashMap<>();
    /** partition/offset/qos/packetId/... */
    @Builder.Default
    private Map<String, Object> protocolMeta = new HashMap<>();
    private long receiveTs;
}
