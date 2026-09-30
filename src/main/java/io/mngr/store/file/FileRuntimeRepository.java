package io.mngr.store.file;

import com.fasterxml.jackson.databind.JsonNode;
import io.mngr.config.MngrProperties;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.AuditEvent;
import io.mngr.core.model.DlqRecord;
import io.mngr.core.model.Page;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.store.DlqQuery;
import io.mngr.store.StoreJson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/** 无库模式运行数据：JSONL 追加写；条件分页查询不支持（显式 8001）。 */
public class FileRuntimeRepository implements RuntimeRepository {

    private final Path dataDir;
    private final AtomicLong dlqSeq = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong auditSeq = new AtomicLong(System.currentTimeMillis());

    public FileRuntimeRepository(MngrProperties props) {
        this.dataDir = Path.of(props.getDataDir(), "data");
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void append(Path file, String jsonLine) {
        try {
            Files.writeString(file, jsonLine + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void appendDlq(DlqRecord r) {
        if (r.getId() == null) r.setId(dlqSeq.incrementAndGet());
        r.setCreatedAt(Instant.now());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.getId());
        m.put("serverId", r.getServerId());
        m.put("topic", r.getTopic());
        m.put("subscriptionId", r.getSubscriptionId());
        m.put("templateId", r.getTemplateId());
        m.put("templateVersion", r.getTemplateVersion());
        m.put("headers", r.getHeaders());
        m.put("payloadB64", Base64.getEncoder().encodeToString(r.getPayload() == null ? new byte[0] : r.getPayload()));
        m.put("errCode", r.getErrCode());
        m.put("errMsg", r.getErrMsg());
        m.put("origin", r.getOrigin());
        m.put("createdAt", r.getCreatedAt().toString());
        append(dataDir.resolve("dlq.jsonl"), StoreJson.write(m));
        rotate(dataDir.resolve("dlq.jsonl"));
    }

    private void rotate(Path file) {
        try {
            if (Files.size(file) > 64L * 1024 * 1024) {
                for (int i = 4; i >= 1; i--) {
                    Path old = file.resolveSibling(file.getFileName() + "." + i);
                    Path moved = file.resolveSibling(file.getFileName() + "." + (i + 1));
                    if (Files.exists(old)) Files.move(old, moved, StandardCopyOption.REPLACE_EXISTING);
                }
                Files.move(file, file.resolveSibling(file.getFileName() + ".1"), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ignored) { }
    }

    List<DlqRecord> scanDlq() {
        List<DlqRecord> out = new ArrayList<>();
        for (int i = 3; i >= 0; i--) {
            Path f = i == 0 ? dataDir.resolve("dlq.jsonl")
                    : dataDir.resolve("dlq.jsonl." + i);
            if (!Files.exists(f)) continue;
            try {
                for (String line : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    JsonNode n = StoreJson.mapper().readTree(line);
                    out.add(fromJson(n));
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return out;
    }

    DlqRecord fromJson(JsonNode n) {
        Map<String, String> headers = new LinkedHashMap<>();
        n.path("headers").fields().forEachRemaining(e -> headers.put(e.getKey(), e.getValue().asText()));
        return DlqRecord.builder()
                .id(n.path("id").asLong())
                .serverId(n.path("serverId").asText(null))
                .topic(n.path("topic").asText(null))
                .subscriptionId(n.path("subscriptionId").asText(null))
                .templateId(n.path("templateId").asText(null))
                .templateVersion(n.has("templateVersion") && !n.get("templateVersion").isNull() ? n.get("templateVersion").asInt() : null)
                .headers(headers)
                .payload(Base64.getDecoder().decode(n.path("payloadB64").asText("")))
                .errCode(n.path("errCode").asInt())
                .errMsg(n.path("errMsg").asText(null))
                .origin(n.path("origin").asText(null))
                .createdAt(Instant.parse(n.path("createdAt").asText()))
                .build();
    }

    @Override
    public Page<DlqRecord> queryDlq(DlqQuery q) {
        throw new MngrException(ErrorCode.FEATURE_DISABLED,
                "file 模式不支持 DLQ 条件分页查询，请使用导出或 /api/v1/dlq/tail");
    }

    /** file 模式降级查询：尾部扫描最近 N 条。 */
    public Page<DlqRecord> tailDlq(int limit) {
        List<DlqRecord> all = scanDlq();
        Collections.reverse(all);
        List<DlqRecord> items = all.stream().limit(limit).toList();
        return new Page<>(items, all.size());
    }

    @Override
    public List<DlqRecord> findDlqByIds(List<Long> ids) {
        Set<Long> want = new HashSet<>(ids);
        return scanDlq().stream().filter(r -> want.contains(r.getId())).toList();
    }

    @Override
    public void deleteDlq(List<Long> ids) {
        Set<Long> want = new HashSet<>(ids);
        List<DlqRecord> keep = scanDlq().stream().filter(r -> !want.contains(r.getId())).toList();
        try {
            Path f = dataDir.resolve("dlq.jsonl");
            Files.deleteIfExists(f);
            for (DlqRecord r : keep) appendDlq(r);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void audit(AuditEvent e) {
        if (e.getId() == null) e.setId(auditSeq.incrementAndGet());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", e.getId());
        m.put("actor", e.getActor());
        m.put("action", e.getAction());
        m.put("objType", e.getObjType());
        m.put("objId", e.getObjId());
        m.put("beforeJson", e.getBeforeJson());
        m.put("afterJson", e.getAfterJson());
        m.put("createdAt", Instant.now().toString());
        append(dataDir.resolve("audit.jsonl"), StoreJson.write(m));
        rotate(dataDir.resolve("audit.jsonl"));
    }

    @Override
    public Page<AuditEvent> queryAudit(int page, int size) {
        throw new MngrException(ErrorCode.FEATURE_DISABLED, "file 模式不支持审计条件查询");
    }

    @Override
    public Set<RuntimeCapability> capabilities() {
        return EnumSet.of(RuntimeCapability.DLQ_REPLAY);
    }
}
