package io.mngr.core.spi;

import io.mngr.core.model.AuditEvent;
import io.mngr.core.model.DlqRecord;
import io.mngr.core.model.Page;
import io.mngr.store.DlqQuery;

import java.util.List;
import java.util.Set;

/** 运行数据 SPI：DLQ / 审计。file 模式以 capabilities 显式声明降级。 */
public interface RuntimeRepository {
    enum RuntimeCapability { DLQ_QUERY, DLQ_REPLAY, AUDIT_QUERY }

    void appendDlq(DlqRecord r);
    Page<DlqRecord> queryDlq(DlqQuery q);
    List<DlqRecord> findDlqByIds(List<Long> ids);
    void deleteDlq(List<Long> ids);

    void audit(AuditEvent e);
    Page<AuditEvent> queryAudit(int page, int size);

    Set<RuntimeCapability> capabilities();
}
