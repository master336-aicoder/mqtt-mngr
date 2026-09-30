package io.mngr.store.file;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.mngr.config.MngrProperties;
import io.mngr.core.enums.*;
import io.mngr.core.model.*;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.store.CredCipher;
import io.mngr.store.StoreJson;
import lombok.Synchronized;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** 无库模式：YAML 单一事实来源，整文件原子重写（temp + ATOMIC_MOVE）。 */
public class FileConfigRepository implements ConfigRepository {

    static final ObjectMapper YAML = new ObjectMapper(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES))
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Path configDir;
    private final CredCipher cipher;
    private final Map<String, List<Map<String, Object>>> cache = new ConcurrentHashMap<>();

    public FileConfigRepository(MngrProperties props, CredCipher cipher) {
        this.configDir = Path.of(props.getDataDir(), "config");
        this.cipher = cipher;
        try {
            Files.createDirectories(configDir.resolve("templates/history"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------- yaml plumbing ----------

    private List<Map<String, Object>> load(String file) {
        return cache.computeIfAbsent(file, f -> {
            Path p = configDir.resolve(f);
            if (!Files.exists(p)) return Collections.synchronizedList(new ArrayList<>());
            try {
                List<Map<String, Object>> l = YAML.readValue(Files.readAllBytes(p), new TypeReference<>() {});
                return Collections.synchronizedList(l == null ? new ArrayList<>() : l);
            } catch (IOException e) {
                throw new IllegalStateException("corrupted yaml " + p, e);
            }
        });
    }

    @Synchronized
    private void flush(String file, List<Map<String, Object>> rows) {
        Path p = configDir.resolve(file);
        Path tmp = p.resolveSibling(file + ".tmp");
        try {
            Files.createDirectories(p.getParent());
            YAML.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), rows);
            try {
                Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("yaml flush failed " + p, e);
        }
    }

    static long epoch(Instant i) { return i == null ? Instant.now().getEpochSecond() : i.getEpochSecond(); }
    static Instant fromEpoch(Object o) { return o == null ? null : Instant.ofEpochSecond(((Number) o).longValue()); }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMap(Object o) {
        return o == null ? new HashMap<>() : new HashMap<>((Map<String, Object>) o);
    }

    // ---------- server ----------

    @Override
    public void saveServer(ServerConfig c) {
        List<Map<String, Object>> rows = load("servers.yaml");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", c.getId());
        row.put("name", c.getName());
        row.put("protocol", c.getProtocol().name());
        row.put("enabled", c.isEnabled());
        row.put("connMode", c.getConnMode().name());
        row.put("conn", c.getConn());
        byte[] enc = cipher.encrypt(c.getCredentials());
        if (enc != null) row.put("credEnc", CredCipher.toBase64(enc));
        row.put("mgmt", c.getMgmt());
        row.put("tags", c.getTags());
        row.put("createdAt", epoch(c.getCreatedAt()));
        row.put("updatedAt", Instant.now().getEpochSecond());
        rows.removeIf(r -> c.getId().equals(r.get("id")));
        rows.add(row);
        flush("servers.yaml", rows);
    }

    @Override
    public Optional<ServerConfig> findServer(String id) {
        return load("servers.yaml").stream().filter(r -> id.equals(r.get("id"))).findFirst().map(this::toServer);
    }

    @Override
    public List<ServerConfig> listServers(String protocol, String keyword, int page, int size) {
        return filterLoad("servers.yaml", protocol, keyword).stream()
                .skip((long) page * size).limit(size).map(this::toServer).toList();
    }

    @Override
    public long countServers(String protocol, String keyword) {
        return filterLoad("servers.yaml", protocol, keyword).size();
    }

    private List<Map<String, Object>> filterLoad(String file, String protocol, String keyword) {
        List<Map<String, Object>> rows = new ArrayList<>(load(file));
        rows.sort(Comparator.comparing(r -> String.valueOf(r.getOrDefault("name", r.get("id")))));
        return rows.stream().filter(r -> {
            if (protocol != null && !protocol.isBlank()
                    && !protocol.toUpperCase().equals(String.valueOf(r.get("protocol")))) return false;
            if (keyword != null && !keyword.isBlank()) {
                String blob = (r.get("name") + " " + r.get("id") + " " + r.get("tags")).toLowerCase();
                return blob.contains(keyword.toLowerCase());
            }
            return true;
        }).toList();
    }

    private ServerConfig toServer(Map<String, Object> r) {
        Map<String, String> creds = new HashMap<>();
        Object ce = r.get("credEnc");
        if (ce != null) creds = cipher.decrypt(CredCipher.fromBase64(String.valueOf(ce)));
        return ServerConfig.builder()
                .id(str(r.get("id"))).name(str(r.get("name")))
                .protocol(Protocol.valueOf(str(r.get("protocol"))))
                .enabled(!Boolean.FALSE.equals(r.get("enabled")))
                .connMode(ConnMode.valueOf(strOrDefault(r.get("connMode"), "LAZY")))
                .conn(asMap(r.get("conn"))).credentials(creds).mgmt(asMap(r.get("mgmt")))
                .tags(str(r.get("tags")))
                .createdAt(fromEpoch(r.get("createdAt"))).updatedAt(fromEpoch(r.get("updatedAt")))
                .build();
    }

    @Override
    public void deleteServer(String id) {
        List<Map<String, Object>> rows = load("servers.yaml");
        rows.removeIf(r -> id.equals(r.get("id")));
        flush("servers.yaml", rows);
    }

    // ---------- topic ----------

    @Override
    public void saveTopic(TopicConfig c) {
        List<Map<String, Object>> rows = load("topics.yaml");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", c.getId());
        row.put("serverId", c.getServerId());
        row.put("namePattern", c.getNamePattern());
        row.put("direction", c.getDirection().name());
        row.put("format", c.getFormat());
        row.put("qos", c.getQos());
        row.put("keyStrategy", c.getKeyStrategy());
        row.put("templateId", c.getTemplateId());
        row.put("autoMatch", c.isAutoMatch());
        row.put("tags", c.getTags());
        row.put("owner", c.getOwner());
        row.put("remark", c.getRemark());
        row.put("deleted", c.isDeleted());
        row.put("updatedAt", Instant.now().getEpochSecond());
        rows.removeIf(r -> c.getId().equals(r.get("id")));
        rows.add(row);
        flush("topics.yaml", rows);
    }

    @Override
    public Optional<TopicConfig> findTopic(String id) {
        return load("topics.yaml").stream()
                .filter(r -> id.equals(r.get("id")) && !Boolean.TRUE.equals(r.get("deleted")))
                .findFirst().map(this::toTopic);
    }

    @Override
    public Optional<TopicConfig> findTopicByName(String serverId, String name) {
        return load("topics.yaml").stream()
                .filter(r -> serverId.equals(r.get("serverId")) && name.equals(r.get("namePattern"))
                        && !Boolean.TRUE.equals(r.get("deleted")))
                .findFirst().map(this::toTopic);
    }

    @Override
    public List<TopicConfig> listTopics(String serverId, String keyword, int page, int size) {
        return activeTopics(serverId, keyword).stream()
                .skip((long) page * size).limit(size).map(this::toTopic).toList();
    }

    @Override
    public long countTopics(String serverId, String keyword) {
        return activeTopics(serverId, keyword).size();
    }

    private List<Map<String, Object>> activeTopics(String serverId, String keyword) {
        List<Map<String, Object>> rows = load("topics.yaml").stream()
                .filter(r -> !Boolean.TRUE.equals(r.get("deleted")))
                .filter(r -> serverId == null || serverId.isBlank() || serverId.equals(r.get("serverId")))
                .filter(r -> {
                    if (keyword == null || keyword.isBlank()) return true;
                    String blob = (r.get("namePattern") + " " + r.get("tags") + " " + r.get("remark")).toLowerCase();
                    return blob.contains(keyword.toLowerCase());
                }).collect(Collectors.toList());
        rows.sort(Comparator.comparing(r -> String.valueOf(r.get("namePattern"))));
        return rows;
    }

    private TopicConfig toTopic(Map<String, Object> r) {
        return TopicConfig.builder()
                .id(str(r.get("id"))).serverId(str(r.get("serverId"))).namePattern(str(r.get("namePattern")))
                .direction(Direction.valueOf(strOrDefault(r.get("direction"), "BOTH")))
                .format(strOrDefault(r.get("format"), "json"))
                .qos(r.get("qos") == null ? null : ((Number) r.get("qos")).intValue())
                .keyStrategy(str(r.get("keyStrategy"))).templateId(str(r.get("templateId")))
                .autoMatch(!Boolean.FALSE.equals(r.get("autoMatch")))
                .tags(str(r.get("tags"))).owner(str(r.get("owner"))).remark(str(r.get("remark")))
                .deleted(Boolean.TRUE.equals(r.get("deleted"))).updatedAt(fromEpoch(r.get("updatedAt")))
                .build();
    }

    @Override
    public void deleteTopic(String id) {
        List<Map<String, Object>> rows = load("topics.yaml");
        rows.stream().filter(r -> id.equals(r.get("id"))).forEach(r -> r.put("deleted", true));
        flush("topics.yaml", rows);
    }

    // ---------- template ----------

    @Override
    public void saveTemplate(TemplateDef t) {
        int version = t.getVersion() > 0 ? t.getVersion() : nextTemplateVersion(t.getId());
        t = t.toBuilder().version(version).build();
        writeYaml(configDir.resolve("templates/" + t.getId() + ".yaml"), templateRow(t));
        writeYaml(configDir.resolve("templates/history/" + t.getId() + "/v" + version + ".yaml"), templateRow(t));
    }

    Map<String, Object> templateRow(TemplateDef t) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", t.getId());
        row.put("version", t.getVersion());
        row.put("name", t.getName());
        row.put("description", t.getDescription());
        row.put("status", t.getStatus().name());
        row.put("fields", StoreJson.mapper().convertValue(t.getFields(), Object.class));
        row.put("source", t.getSource());
        row.put("owner", t.getOwner());
        row.put("tags", t.getTags());
        row.put("createdAt", epoch(t.getCreatedAt()));
        row.put("updatedAt", Instant.now().getEpochSecond());
        return row;
    }

    private void writeYaml(Path p, Object v) {
        Path tmp = p.resolveSibling(p.getFileName() + ".tmp");
        try {
            Files.createDirectories(p.getParent());
            YAML.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), v);
            try {
                Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private int nextTemplateVersion(String id) {
        Path dir = configDir.resolve("templates/history/" + id);
        if (!Files.isDirectory(dir)) return 1;
        try (var s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> n.startsWith("v") && n.endsWith(".yaml"))
                    .map(n -> Integer.parseInt(n.substring(1, n.length() - 5)))
                    .max(Integer::compare).orElse(0) + 1;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Optional<TemplateDef> findTemplate(String id) {
        Path p = configDir.resolve("templates/" + id + ".yaml");
        if (!Files.exists(p)) return Optional.empty();
        try {
            return Optional.of(toTemplate(YAML.readValue(Files.readAllBytes(p), new TypeReference<>() {})));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Optional<TemplateDef> findTemplate(String id, int version) {
        Path p = configDir.resolve("templates/history/" + id + "/v" + version + ".yaml");
        if (!Files.exists(p)) return Optional.empty();
        try {
            return Optional.of(toTemplate(YAML.readValue(Files.readAllBytes(p), new TypeReference<>() {})));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private TemplateDef toTemplate(Map<String, Object> r) {
        List<FieldDef> fields = r.get("fields") == null ? List.of()
                : StoreJson.mapper().convertValue(r.get("fields"), new TypeReference<>() {});
        return TemplateDef.builder()
                .id(str(r.get("id"))).version(r.get("version") == null ? 1 : ((Number) r.get("version")).intValue())
                .name(str(r.get("name"))).description(str(r.get("description")))
                .status(TemplateStatus.valueOf(strOrDefault(r.get("status"), "ENABLED")))
                .fields(fields).source(str(r.get("source"))).owner(str(r.get("owner"))).tags(str(r.get("tags")))
                .createdAt(fromEpoch(r.get("createdAt"))).updatedAt(fromEpoch(r.get("updatedAt")))
                .build();
    }

    @Override
    public List<TemplateDef> listTemplates(String keyword, int page, int size) {
        Path dir = configDir.resolve("templates");
        List<TemplateDef> all = new ArrayList<>();
        try (var s = Files.list(dir)) {
            s.filter(p -> p.getFileName().toString().endsWith(".yaml"))
                    .forEach(p -> {
                        try {
                            all.add(toTemplate(YAML.readValue(Files.readAllBytes(p), new TypeReference<>() {})));
                        } catch (IOException ignored) { }
                    });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        if (keyword != null && !keyword.isBlank()) {
            String kw = keyword.toLowerCase();
            all.removeIf(t -> !(t.getName() + " " + t.getId() + " " + t.getTags()).toLowerCase().contains(kw));
        }
        all.sort(Comparator.comparing(TemplateDef::getUpdatedAt, Comparator.nullsLast(Comparator.reverseOrder())));
        return all.stream().skip((long) page * size).limit(size).toList();
    }

    @Override
    public long countTemplates(String keyword) {
        return listTemplates(keyword, 0, Integer.MAX_VALUE).size();
    }

    @Override
    public List<TemplateDef> listTemplateVersions(String id) {
        Path dir = configDir.resolve("templates/history/" + id);
        List<TemplateDef> out = new ArrayList<>();
        if (!Files.isDirectory(dir)) return out;
        try (var s = Files.list(dir)) {
            s.forEach(p -> {
                try {
                    out.add(toTemplate(YAML.readValue(Files.readAllBytes(p), new TypeReference<>() {})));
                } catch (IOException ignored) { }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        out.sort(Comparator.comparingInt(TemplateDef::getVersion).reversed());
        return out;
    }

    @Override
    public void deleteTemplate(String id) {
        try {
            Files.deleteIfExists(configDir.resolve("templates/" + id + ".yaml"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void updateTemplateStatus(String id, int version, TemplateStatus status) {
        findTemplate(id, version > 0 ? version : nextTemplateVersion(id) - 1).ifPresent(t -> {
            saveTemplate(t.toBuilder().status(status).version(t.getVersion()).build());
        });
    }

    // ---------- subscription ----------

    @Override
    public void saveSubscription(SubscriptionDef d) {
        List<Map<String, Object>> rows = load("subscriptions.yaml");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", d.getId());
        row.put("serverId", d.getServerId());
        row.put("topicId", d.getTopicId());
        row.put("topic", d.getTopic());
        row.put("group", d.getGroup());
        row.put("state", d.getState());
        row.put("qos", d.getQos());
        row.put("templateIds", d.getTemplateIds());
        row.put("autoMatch", d.isAutoMatch());
        row.put("retryMax", d.getRetryMax());
        row.put("dlqEnabled", d.isDlqEnabled());
        row.put("persistent", d.isPersistent());
        row.put("offsetStrategy", d.getOffsetStrategy());
        row.put("offsetTimestamp", d.getOffsetTimestamp());
        row.put("createdAt", epoch(d.getCreatedAt()));
        rows.removeIf(r -> d.getId().equals(r.get("id")));
        rows.add(row);
        flush("subscriptions.yaml", rows);
    }

    @Override
    public Optional<SubscriptionDef> findSubscription(String id) {
        return load("subscriptions.yaml").stream().filter(r -> id.equals(r.get("id"))).findFirst().map(this::toSub);
    }

    @Override
    public List<SubscriptionDef> listSubscriptions(String serverId) {
        return load("subscriptions.yaml").stream()
                .filter(r -> serverId == null || serverId.isBlank() || serverId.equals(r.get("serverId")))
                .map(this::toSub).toList();
    }

    @Override
    public void deleteSubscription(String id) {
        List<Map<String, Object>> rows = load("subscriptions.yaml");
        rows.removeIf(r -> id.equals(r.get("id")));
        flush("subscriptions.yaml", rows);
    }

    private SubscriptionDef toSub(Map<String, Object> r) {
        return SubscriptionDef.builder()
                .id(str(r.get("id"))).serverId(str(r.get("serverId"))).topicId(str(r.get("topicId")))
                .topic(str(r.get("topic"))).group(str(r.get("group")))
                .state(strOrDefault(r.get("state"), "STOPPED"))
                .qos(r.get("qos") == null ? null : ((Number) r.get("qos")).intValue())
                .templateIds(r.get("templateIds") == null ? null
                        : ((List<?>) r.get("templateIds")).stream().map(String::valueOf).toList())
                .autoMatch(!Boolean.FALSE.equals(r.get("autoMatch")))
                .retryMax(r.get("retryMax") == null ? 2 : ((Number) r.get("retryMax")).intValue())
                .dlqEnabled(!Boolean.FALSE.equals(r.get("dlqEnabled")))
                .persistent(!Boolean.FALSE.equals(r.get("persistent")))
                .offsetStrategy(strOrDefault(r.get("offsetStrategy"), "LATEST"))
                .offsetTimestamp(r.get("offsetTimestamp") == null ? null : ((Number) r.get("offsetTimestamp")).longValue())
                .createdAt(fromEpoch(r.get("createdAt")))
                .build();
    }

    static String str(Object o) { return o == null ? null : String.valueOf(o); }
    static String strOrDefault(Object o, String def) { return o == null ? def : String.valueOf(o); }
}
