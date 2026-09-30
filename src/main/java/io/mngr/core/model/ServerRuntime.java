package io.mngr.core.model;

import io.mngr.core.enums.ServerState;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/** Server 运行态（内存态，不持久化）。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder(toBuilder = true)
public class ServerRuntime {
    private String serverId;
    @Builder.Default
    private ServerState state = ServerState.INIT;
    private Instant connectedSince;
    private int retryCount;
    private String lastError;
    private Instant lastErrorAt;
    private Map<String, Object> protocolInfo;  // broker 版本、节点数等
}
