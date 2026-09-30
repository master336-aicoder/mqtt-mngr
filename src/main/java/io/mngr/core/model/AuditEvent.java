package io.mngr.core.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/** 审计事件（FR-7.4）。 */
@lombok.extern.jackson.Jacksonized
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
public class AuditEvent {
    private Long id;
    private String actor;
    private String action;       // CREATE|UPDATE|DELETE|CONNECT|DISCONNECT|MGMT_OP|REPLAY
    private String objType;      // SERVER|TOPIC|TEMPLATE|SUBSCRIPTION|DLQ
    private String objId;
    private String beforeJson;
    private String afterJson;
    private Instant createdAt;
}
