package io.mngr.store.h2;

import com.fasterxml.jackson.core.type.TypeReference;
import io.mngr.core.model.AuditEvent;
import io.mngr.core.model.DlqRecord;
import io.mngr.core.model.Page;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.store.DlqQuery;
import io.mngr.store.StoreJson;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.EnumSet;
import java.util.Set;

/** 仅在 h2 模式下由 CoreConfig 条件装配。 */
@RequiredArgsConstructor
public class H2RuntimeRepository implements RuntimeRepository {

    private final JdbcTemplate jdbc;

    @Override
    public void appendDlq(DlqRecord r) {
        jdbc.update("INSERT INTO dlq_message(server_id,topic,subscription_id,template_id,template_version,headers_json,payload,err_code,err_msg,origin,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                r.getServerId(), r.getTopic(), r.getSubscriptionId(), r.getTemplateId(), r.getTemplateVersion(),
                StoreJson.write(r.getHeaders()), r.getPayload(), r.getErrCode(),
                trim(r.getErrMsg(), 1000), r.getOrigin(), Timestamp.from(Instant.now()));
    }

    static String trim(String s, int max) { return s == null ? null : (s.length() <= max ? s : s.substring(0, max)); }

    @Override
    public Page<DlqRecord> queryDlq(DlqQuery q) {
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (q.getServerId() != null && !q.getServerId().isBlank()) { where.append(" AND server_id=?"); args.add(q.getServerId()); }
        if (q.getTopic() != null && !q.getTopic().isBlank()) { where.append(" AND topic LIKE ?"); args.add("%" + q.getTopic() + "%"); }
        if (q.getSubscriptionId() != null && !q.getSubscriptionId().isBlank()) { where.append(" AND subscription_id=?"); args.add(q.getSubscriptionId()); }
        if (q.getErrCode() != null) { where.append(" AND err_code=?"); args.add(q.getErrCode()); }
        if (q.getFrom() != null) { where.append(" AND created_at>=?"); args.add(Timestamp.from(q.getFrom())); }
        if (q.getTo() != null) { where.append(" AND created_at<=?"); args.add(Timestamp.from(q.getTo())); }
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM dlq_message" + where, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(q.getSize());
        pageArgs.add((long) q.getPage() * q.getSize());
        List<DlqRecord> items = jdbc.query(
                "SELECT * FROM dlq_message" + where + " ORDER BY id DESC LIMIT ? OFFSET ?",
                DLQ_MAPPER, pageArgs.toArray());
        return new Page<>(items, total == null ? 0 : total);
    }

    @Override
    public List<DlqRecord> findDlqByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        String ph = String.join(",", ids.stream().map(i -> "?").toList());
        return jdbc.query("SELECT * FROM dlq_message WHERE id IN (" + ph + ") ORDER BY id", DLQ_MAPPER, ids.toArray());
    }

    @Override
    public void deleteDlq(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return;
        String ph = String.join(",", ids.stream().map(i -> "?").toList());
        jdbc.update("DELETE FROM dlq_message WHERE id IN (" + ph + ")", ids.toArray());
    }

    @Override
    public void audit(AuditEvent e) {
        jdbc.update("INSERT INTO audit_log(actor,action,obj_type,obj_id,before_json,after_json,created_at) VALUES(?,?,?,?,?,?,?)",
                e.getActor(), e.getAction(), e.getObjType(), e.getObjId(),
                e.getBeforeJson(), e.getAfterJson(), Timestamp.from(Instant.now()));
    }

    @Override
    public Page<AuditEvent> queryAudit(int page, int size) {
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM audit_log", Long.class);
        List<AuditEvent> items = jdbc.query(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ? OFFSET ?", AUDIT_MAPPER, size, (long) page * size);
        return new Page<>(items, total == null ? 0 : total);
    }

    @Override
    public Set<RuntimeCapability> capabilities() {
        return EnumSet.allOf(RuntimeCapability.class);
    }

    static final RowMapper<DlqRecord> DLQ_MAPPER = (ResultSet rs, int i) -> {
        Map<String, String> headers = StoreJson.read(rs.getString("headers_json"), new TypeReference<>() {});
        return DlqRecord.builder()
                .id(rs.getLong("id"))
                .serverId(rs.getString("server_id"))
                .topic(rs.getString("topic"))
                .subscriptionId(rs.getString("subscription_id"))
                .templateId(rs.getString("template_id"))
                .templateVersion((Integer) rs.getObject("template_version"))
                .headers(headers == null ? Map.of() : headers)
                .payload(readBytes(rs))
                .errCode(rs.getInt("err_code"))
                .errMsg(rs.getString("err_msg"))
                .origin(rs.getString("origin"))
                .createdAt(toInstant(rs.getTimestamp("created_at")))
                .build();
    };

    static byte[] readBytes(ResultSet rs) {
        try { return rs.getBytes("payload"); } catch (Exception e) { return new byte[0]; }
    }

    static final RowMapper<AuditEvent> AUDIT_MAPPER = (ResultSet rs, int i) -> AuditEvent.builder()
            .id(rs.getLong("id"))
            .actor(rs.getString("actor"))
            .action(rs.getString("action"))
            .objType(rs.getString("obj_type"))
            .objId(rs.getString("obj_id"))
            .beforeJson(rs.getString("before_json"))
            .afterJson(rs.getString("after_json"))
            .createdAt(toInstant(rs.getTimestamp("created_at")))
            .build();

    static Instant toInstant(Timestamp t) { return t == null ? null : t.toInstant(); }
}
