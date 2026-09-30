package io.mngr.web;

import io.mngr.core.model.PublishRequest;
import io.mngr.core.model.PublishResult;
import io.mngr.core.pub.PublisherEngine;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class PublishController {

    private final PublisherEngine publisherEngine;
    private final AuditHelper audit;

    @PostMapping("/publish")
    public ApiResponse<PublishResult> publish(@RequestBody PublishRequest req) {
        PublishResult r = publisherEngine.publish(req);
        if (r.isSuccess()) {
            audit.record("PUBLISH", "MESSAGE", req.getServerId() + "/" + req.getTopic(), null,
                    java.util.Map.of("messageId", String.valueOf(r.getMessageId())));
            return ApiResponse.ok(r);
        }
        return new ApiResponse<>(r.getErrorCode() == null ? 4001 : r.getErrorCode(),
                r.getErrorMessage(), r, java.util.UUID.randomUUID().toString());
    }
}
