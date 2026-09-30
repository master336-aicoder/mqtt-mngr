package io.mngr.web;

import io.mngr.core.model.AuditEvent;
import io.mngr.core.spi.RuntimeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AuditHelper {

    private final RuntimeRepository runtime;

    public void record(String action, String objType, String objId, Object before, Object after) {
        try {
            runtime.audit(AuditEvent.builder()
                    .actor("web")
                    .action(action)
                    .objType(objType)
                    .objId(objId)
                    .beforeJson(before == null ? null : io.mngr.store.StoreJson.write(before))
                    .afterJson(after == null ? null : io.mngr.store.StoreJson.write(after))
                    .build());
        } catch (Exception ignored) {
            // 审计失败不阻断主流程
        }
    }
}
