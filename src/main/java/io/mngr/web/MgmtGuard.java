package io.mngr.web;

import io.mngr.config.MngrProperties;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.ServerConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 管理面门控（FR-2.4/2.5、NFR 安全）：WRITE 需 allowAdminOps；DANGER 需 confirm 回显资源名。 */
@Component
@RequiredArgsConstructor
public class MgmtGuard {

    private final MngrProperties props;

    /** 全局开关或 server.mgmt.allowAdminOps 覆盖。 */
    public boolean adminAllowed(ServerConfig cfg) {
        Object v = cfg.getMgmt().get("allowAdminOps");
        if (v != null) return Boolean.parseBoolean(String.valueOf(v));
        return props.isAllowAdminOps();
    }

    public void requireWrite(ServerConfig cfg) {
        if (!adminAllowed(cfg)) {
            throw MngrException.of(ErrorCode.ADMIN_DISABLED, "server=%s 管理操作未开启", cfg.getId());
        }
    }

    /** 危险操作：confirm 必须与资源名一致，否则 7002。 */
    public void requireDanger(ServerConfig cfg, String resource, String confirm) {
        requireWrite(cfg);
        if (confirm == null || !confirm.equals(resource)) {
            throw MngrException.of(ErrorCode.CONFIRM_REQUIRED,
                    "请将资源名 [%s] 原样填入 confirm 参数以确认此操作", resource);
        }
    }
}
