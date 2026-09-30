package io.mngr.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.mngr.core.enums.ConnMode;
import io.mngr.core.enums.Protocol;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** 集群/接入点配置（FR-1）。凭据独立于 conn 参数，存储层负责加密。 */
@Data
@Builder(toBuilder = true)
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerConfig {
    private String id;
    private String name;
    private Protocol protocol;
    @Builder.Default
    private boolean enabled = true;
    @Builder.Default
    private ConnMode connMode = ConnMode.LAZY;
    /** 非敏感连接参数：bootstrapServers/serverUrls/clientId/keepAlive/qos/sessionExpiry/tls... */
    @Builder.Default
    private Map<String, Object> conn = new HashMap<>();
    /** 敏感凭据：username/password/clientSecret/mgmtPassword 等，Repository 层 AES-GCM 加密。 */
    @Builder.Default
    private Map<String, String> credentials = new HashMap<>();
    /** 管理面配置（FR-2.4/2.5）：Kafka allowAdminOps 覆盖、EMQX baseUrl 等。 */
    @Builder.Default
    private Map<String, Object> mgmt = new HashMap<>();
    private String tags;
    private Instant createdAt;
    private Instant updatedAt;

    public String connStr(String key, String def) {
        Object v = conn.get(key);
        return v == null ? def : String.valueOf(v);
    }

    public int connInt(String key, int def) {
        Object v = conn.get(key);
        if (v == null) return def;
        try { return Integer.parseInt(String.valueOf(v)); } catch (NumberFormatException e) { return def; }
    }

    public boolean connBool(String key, boolean def) {
        Object v = conn.get(key);
        if (v == null) return def;
        return Boolean.parseBoolean(String.valueOf(v));
    }

    public Object mgmtObj(String key) { return mgmt.get(key); }
}
