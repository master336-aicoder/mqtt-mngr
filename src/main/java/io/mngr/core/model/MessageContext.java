package io.mngr.core.model;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.core.enums.MatchType;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/** 分发管线的消息上下文（FR-4/FR-6）。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
public class MessageContext {
    private String serverId;
    private String subscriptionId;
    private String topic;
    private Map<String, String> headers;
    private Map<String, Object> protocolMeta;
    private long receiveTs;

    private JsonNode raw;               // 解析后的 JSON 树
    private byte[] rawBytes;
    private String parseError;          // 非空 = 解析失败

    private String templateId;
    private Integer templateVersion;
    @Builder.Default
    private MatchType matchType = MatchType.NONE;
    private Double matchScore;
    private TypedPayload typed;

    public boolean isParsed() { return parseError == null && raw != null; }
}
