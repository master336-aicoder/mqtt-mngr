package io.mngr.store;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class DlqQuery {
    private String serverId;
    private String topic;
    private String subscriptionId;
    private Integer errCode;
    private Instant from;
    private Instant to;
    @Builder.Default
    private int page = 0;
    @Builder.Default
    private int size = 20;
}
