package io.mngr.store.h2;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
import io.mngr.core.enums.*;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.*;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.store.CredCipher;
import io.mngr.store.StoreJson;
import io.mngr.store.h2.entity.*;
import io.mngr.store.h2.mapper.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/** 仅在 h2 模式下由 CoreConfig 条件装配（不标注 @Repository，避免 file 模式加载）。 */
@RequiredArgsConstructor
public class H2ConfigRepository implements ConfigRepository {

    private final ServerMapper serverMapper;
    private final TopicMapper topicMapper;
    private final TemplateMapper templateMapper;
    private final SubscriptionMapper subscriptionMapper;
    private final CredCipher cipher;

    static LocalDateTime toLdt(Instant i) { return i == null ? LocalDateTime.now() : LocalDateTime.ofInstant(i, ZoneOffset.UTC); }
    static Instant toInstant(LocalDateTime t) { return t == null ? null : t.toInstant(ZoneOffset.UTC); }

    // ---------------- server ----------------

    @Override
    public void saveServer(ServerConfig c) {
        ServerEntity e = new ServerEntity();
        e.setId(c.getId());
        e.setName(c.getName());
        e.setProtocol(c.getProtocol().name());
        e.setEnabled(c.isEnabled());
        e.setConnMode(c.getConnMode().name());
        e.setConnJson(StoreJson.write(c.getConn()));
        e.setCredEnc(cipher.encrypt(c.getCredentials()));
        e.setMgmtJson(StoreJson.write(c.getMgmt()));
        e.setTags(c.getTags());
        e.setCreatedAt(toLdt(c.getCreatedAt()));
        e.setUpdatedAt(LocalDateTime.now());
        if (serverMapper.selectById(c.getId()) == null) serverMapper.insert(e);
        else serverMapper.update(e, new QueryWrapper<ServerEntity>().eq("id", c.getId()));
    }

    @Override
    public Optional<ServerConfig> findServer(String id) {
        return Optional.ofNullable(serverMapper.selectById(id)).map(this::toModel);
    }

    @Override
    public List<ServerConfig> listServers(String protocol, String keyword, int page, int size) {
        QueryWrapper<ServerEntity> w = filterServers(protocol, keyword);
        w.orderByAsc("name").last("LIMIT " + size + " OFFSET " + (long) page * size);
        return serverMapper.selectList(w).stream().map(this::toModel).toList();
    }

    @Override
    public long countServers(String protocol, String keyword) {
        return serverMapper.selectCount(filterServers(protocol, keyword));
    }

    private QueryWrapper<ServerEntity> filterServers(String protocol, String keyword) {
        QueryWrapper<ServerEntity> w = new QueryWrapper<>();
        if (protocol != null && !protocol.isBlank()) w.eq("protocol", protocol.toUpperCase());
        if (keyword != null && !keyword.isBlank()) w.and(q -> q.like("name", keyword).or().like("id", keyword).or().like("tags", keyword));
        return w;
    }

    @Override
    public void deleteServer(String id) {
        serverMapper.delete(new QueryWrapper<ServerEntity>().eq("id", id));
    }

    private ServerConfig toModel(ServerEntity e) {
        return ServerConfig.builder()
                .id(e.getId()).name(e.getName()).protocol(Protocol.valueOf(e.getProtocol()))
                .enabled(Boolean.TRUE.equals(e.getEnabled()))
                .connMode(ConnMode.valueOf(e.getConnMode()))
                .conn(json(e.getConnJson()))
                .credentials(e.getCredEnc() == null ? new HashMap<>() : cipher.decrypt(e.getCredEnc()))
                .mgmt(json(e.getMgmtJson()))
                .tags(e.getTags())
                .createdAt(toInstant(e.getCreatedAt())).updatedAt(toInstant(e.getUpdatedAt()))
                .build();
    }

    static Map<String, Object> json(String s) {
        Map<String, Object> m = StoreJson.read(s, new TypeReference<>() {});
        return m == null ? new HashMap<>() : m;
    }

    // ---------------- topic ----------------

    @Override
    public void saveTopic(TopicConfig c) {
        TopicEntity e = new TopicEntity();
        e.setId(c.getId());
        e.setServerId(c.getServerId());
        e.setNamePattern(c.getNamePattern());
        e.setDirection(c.getDirection().name());
        e.setFormat(c.getFormat());
        e.setQos(c.getQos());
        e.setKeyStrategy(c.getKeyStrategy());
        e.setTemplateId(c.getTemplateId());
        e.setAutoMatch(c.isAutoMatch());
        e.setTags(c.getTags());
        e.setOwner(c.getOwner());
        e.setRemark(c.getRemark());
        e.setDeleted(c.isDeleted());
        e.setUpdatedAt(LocalDateTime.now());
        if (topicMapper.selectById(c.getId()) == null) topicMapper.insert(e);
        else topicMapper.update(e, new QueryWrapper<TopicEntity>().eq("id", c.getId()));
    }

    @Override
    public Optional<TopicConfig> findTopic(String id) {
        TopicEntity e = topicMapper.selectById(id);
        return (e == null || Boolean.TRUE.equals(e.getDeleted())) ? Optional.empty() : Optional.of(toModel(e));
    }

    @Override
    public Optional<TopicConfig> findTopicByName(String serverId, String name) {
        return topicMapper.selectList(new QueryWrapper<TopicEntity>()
                        .eq("server_id", serverId).eq("name_pattern", name).eq("deleted", false))
                .stream().findFirst().map(this::toModel);
    }

    @Override
    public List<TopicConfig> listTopics(String serverId, String keyword, int page, int size) {
        QueryWrapper<TopicEntity> w = filterTopics(serverId, keyword);
        w.eq("deleted", false).orderByAsc("name_pattern").last("LIMIT " + size + " OFFSET " + (long) page * size);
        return topicMapper.selectList(w).stream().map(this::toModel).toList();
    }

    @Override
    public long countTopics(String serverId, String keyword) {
        return topicMapper.selectCount(filterTopics(serverId, keyword).eq("deleted", false));
    }

    private QueryWrapper<TopicEntity> filterTopics(String serverId, String keyword) {
        QueryWrapper<TopicEntity> w = new QueryWrapper<>();
        if (serverId != null && !serverId.isBlank()) w.eq("server_id", serverId);
        if (keyword != null && !keyword.isBlank()) w.and(q -> q.like("name_pattern", keyword).or().like("tags", keyword).or().like("remark", keyword));
        return w;
    }

    @Override
    public void deleteTopic(String id) {
        TopicEntity e = new TopicEntity();
        e.setDeleted(true);
        e.setUpdatedAt(LocalDateTime.now());
        topicMapper.update(e, new QueryWrapper<TopicEntity>().eq("id", id));
    }

    private TopicConfig toModel(TopicEntity e) {
        return TopicConfig.builder()
                .id(e.getId()).serverId(e.getServerId()).namePattern(e.getNamePattern())
                .direction(Direction.valueOf(e.getDirection())).format(e.getFormat())
                .qos(e.getQos()).keyStrategy(e.getKeyStrategy()).templateId(e.getTemplateId())
                .autoMatch(Boolean.TRUE.equals(e.getAutoMatch()))
                .tags(e.getTags()).owner(e.getOwner()).remark(e.getRemark())
                .deleted(Boolean.TRUE.equals(e.getDeleted())).updatedAt(toInstant(e.getUpdatedAt()))
                .build();
    }

    // ---------------- template ----------------

    @Data
    static class FieldsHolder {
        public List<FieldDef> fields;
        FieldsHolder() {}
        FieldsHolder(List<FieldDef> f) { this.fields = f; }
    }

    @Override
    public void saveTemplate(TemplateDef t) {
        int version = t.getVersion() > 0 ? t.getVersion() : nextVersion(t.getId());
        TemplateEntity e = new TemplateEntity();
        e.setId(t.getId());
        e.setVersion(version);
        e.setName(t.getName());
        e.setDescription(t.getDescription());
        e.setStatus(t.getStatus().name());
        e.setFieldsJson(StoreJson.write(new FieldsHolder(t.getFields())));
        e.setSource(t.getSource());
        e.setOwner(t.getOwner());
        e.setTags(t.getTags());
        e.setDeleted(false);
        e.setCreatedAt(toLdt(t.getCreatedAt()));
        e.setUpdatedAt(LocalDateTime.now());
        if (findTemplate(t.getId(), version).isPresent()) {
            templateMapper.update(e, new QueryWrapper<TemplateEntity>().eq("id", t.getId()).eq("version", version));
        } else {
            templateMapper.insert(e);
        }
    }

    private int nextVersion(String id) {
        return templateMapper.selectList(new QueryWrapper<TemplateEntity>().eq("id", id))
                .stream().mapToInt(TemplateEntity::getVersion).max().orElse(0) + 1;
    }

    @Override
    public Optional<TemplateDef> findTemplate(String id) {
        List<TemplateEntity> all = templateMapper.selectList(new QueryWrapper<TemplateEntity>()
                .eq("id", id).eq("deleted", false).orderByDesc("version").last("LIMIT 1"));
        return all.stream().findFirst().map(this::toModel);
    }

    @Override
    public Optional<TemplateDef> findTemplate(String id, int version) {
        return templateMapper.selectList(new QueryWrapper<TemplateEntity>()
                        .eq("id", id).eq("version", version).eq("deleted", false))
                .stream().findFirst().map(this::toModel);
    }

    @Override
    public List<TemplateDef> listTemplates(String keyword, int page, int size) {
        return latestGrouped(keyword).stream().skip((long) page * size).limit(size).map(this::toModel).toList();
    }

    @Override
    public long countTemplates(String keyword) {
        return latestGrouped(keyword).size();
    }

    /** 每个模板 id 取最高未删版本（数据量小，内存分组即可）。 */
    private List<TemplateEntity> latestGrouped(String keyword) {
        QueryWrapper<TemplateEntity> w = new QueryWrapper<TemplateEntity>().eq("deleted", false);
        if (keyword != null && !keyword.isBlank()) {
            w.and(q -> q.like("name", keyword).or().like("id", keyword).or().like("tags", keyword));
        }
        Map<String, TemplateEntity> latest = new TreeMap<>();
        for (TemplateEntity e : templateMapper.selectList(w.orderByAsc("version"))) {
            latest.put(e.getId(), e);
        }
        List<TemplateEntity> out = new ArrayList<>(latest.values());
        out.sort(Comparator.comparing(TemplateEntity::getUpdatedAt, Comparator.nullsLast(Comparator.reverseOrder())));
        return out;
    }

    @Override
    public List<TemplateDef> listTemplateVersions(String id) {
        return templateMapper.selectList(new QueryWrapper<TemplateEntity>()
                .eq("id", id).eq("deleted", false).orderByDesc("version")).stream().map(this::toModel).toList();
    }

    @Override
    public void deleteTemplate(String id) {
        TemplateEntity u = new TemplateEntity();
        u.setDeleted(true);
        templateMapper.update(u, new QueryWrapper<TemplateEntity>().eq("id", id));
    }

    @Override
    public void updateTemplateStatus(String id, int version, TemplateStatus status) {
        TemplateEntity u = new TemplateEntity();
        u.setStatus(status.name());
        u.setUpdatedAt(LocalDateTime.now());
        QueryWrapper<TemplateEntity> w = new QueryWrapper<TemplateEntity>().eq("id", id);
        if (version > 0) w.eq("version", version);
        else w.eq("version", nextVersion(id) - 1);
        templateMapper.update(u, w);
    }

    private TemplateDef toModel(TemplateEntity e) {
        FieldsHolder h = StoreJson.read(e.getFieldsJson(), new TypeReference<>() {});
        return TemplateDef.builder()
                .id(e.getId()).version(e.getVersion()).name(e.getName()).description(e.getDescription())
                .status(TemplateStatus.valueOf(e.getStatus()))
                .fields(h == null || h.fields == null ? List.of() : h.fields)
                .source(e.getSource()).owner(e.getOwner()).tags(e.getTags())
                .createdAt(toInstant(e.getCreatedAt())).updatedAt(toInstant(e.getUpdatedAt()))
                .build();
    }

    // ---------------- subscription ----------------

    @Data
    static class SubOptions {
        public Integer qos;
        public List<String> templateIds;
        public boolean autoMatch = true;
        public int retryMax = 2;
        public boolean dlqEnabled = true;
        public boolean persistent = true;
        public String offsetStrategy = "LATEST";
        public Long offsetTimestamp;
    }

    @Override
    public void saveSubscription(SubscriptionDef d) {
        SubscriptionEntity e = new SubscriptionEntity();
        e.setId(d.getId());
        e.setServerId(d.getServerId());
        e.setTopicId(d.getTopicId());
        e.setTopic(d.getTopic());
        e.setGrp(d.getGroup());
        e.setState(d.getState());
        SubOptions o = new SubOptions();
        o.qos = d.getQos(); o.templateIds = d.getTemplateIds(); o.autoMatch = d.isAutoMatch();
        o.retryMax = d.getRetryMax(); o.dlqEnabled = d.isDlqEnabled(); o.persistent = d.isPersistent();
        o.offsetStrategy = d.getOffsetStrategy(); o.offsetTimestamp = d.getOffsetTimestamp();
        e.setOptionsJson(StoreJson.write(o));
        e.setCreatedAt(toLdt(d.getCreatedAt()));
        if (subscriptionMapper.selectById(d.getId()) == null) subscriptionMapper.insert(e);
        else subscriptionMapper.update(e, new QueryWrapper<SubscriptionEntity>().eq("id", d.getId()));
    }

    @Override
    public Optional<SubscriptionDef> findSubscription(String id) {
        return Optional.ofNullable(subscriptionMapper.selectById(id)).map(this::toModel);
    }

    @Override
    public List<SubscriptionDef> listSubscriptions(String serverId) {
        QueryWrapper<SubscriptionEntity> w = new QueryWrapper<>();
        if (serverId != null && !serverId.isBlank()) w.eq("server_id", serverId);
        return subscriptionMapper.selectList(w.orderByAsc("created_at")).stream().map(this::toModel).toList();
    }

    @Override
    public void deleteSubscription(String id) {
        subscriptionMapper.delete(new QueryWrapper<SubscriptionEntity>().eq("id", id));
    }

    private SubscriptionDef toModel(SubscriptionEntity e) {
        SubOptions o = StoreJson.read(e.getOptionsJson(), new TypeReference<>() {});
        if (o == null) o = new SubOptions();
        return SubscriptionDef.builder()
                .id(e.getId()).serverId(e.getServerId()).topicId(e.getTopicId()).topic(e.getTopic())
                .group(e.getGrp()).state(e.getState())
                .qos(o.qos).templateIds(o.templateIds).autoMatch(o.autoMatch).retryMax(o.retryMax)
                .dlqEnabled(o.dlqEnabled).persistent(o.persistent)
                .offsetStrategy(o.offsetStrategy).offsetTimestamp(o.offsetTimestamp)
                .createdAt(toInstant(e.getCreatedAt()))
                .build();
    }
}
