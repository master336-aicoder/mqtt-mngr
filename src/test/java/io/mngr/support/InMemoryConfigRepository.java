package io.mngr.support;

import io.mngr.core.enums.TemplateStatus;
import io.mngr.core.model.*;
import io.mngr.core.spi.ConfigRepository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** 单元测试用内存实现（仅模板/订阅相关方法有真实语义）。 */
public class InMemoryConfigRepository implements ConfigRepository {

    private final Map<String, List<TemplateDef>> templates = new ConcurrentHashMap<>();
    private final Map<String, SubscriptionDef> subs = new ConcurrentHashMap<>();
    private final Map<String, TopicConfig> topics = new ConcurrentHashMap<>();

    public void putTemplate(TemplateDef t) {
        templates.computeIfAbsent(t.getId(), k -> new ArrayList<>()).add(t);
    }

    @Override public void saveServer(ServerConfig c) { throw new UnsupportedOperationException(); }
    @Override public Optional<ServerConfig> findServer(String id) { return Optional.empty(); }
    @Override public List<ServerConfig> listServers(String p, String k, int pg, int sz) { return List.of(); }
    @Override public long countServers(String p, String k) { return 0; }
    @Override public void deleteServer(String id) { }

    @Override public void saveTopic(TopicConfig c) { topics.put(c.getId(), c); }
    @Override public Optional<TopicConfig> findTopic(String id) { return Optional.ofNullable(topics.get(id)); }
    @Override public Optional<TopicConfig> findTopicByName(String serverId, String name) {
        return topics.values().stream().filter(t -> t.getServerId().equals(serverId) && t.getNamePattern().equals(name)).findFirst();
    }
    @Override public List<TopicConfig> listTopics(String serverId, String keyword, int page, int size) {
        return new ArrayList<>(topics.values());
    }
    @Override public long countTopics(String serverId, String keyword) { return topics.size(); }
    @Override public void deleteTopic(String id) { topics.remove(id); }

    @Override
    public void saveTemplate(TemplateDef t) {
        List<TemplateDef> vs = templates.computeIfAbsent(t.getId(), k -> new ArrayList<>());
        int v = vs.stream().mapToInt(TemplateDef::getVersion).max().orElse(0) + 1;
        t.setVersion(v);
        vs.add(t);
    }
    @Override
    public Optional<TemplateDef> findTemplate(String id) {
        List<TemplateDef> vs = templates.get(id);
        if (vs == null || vs.isEmpty()) return Optional.empty();
        return vs.stream()
                .filter(t -> t.getStatus() == TemplateStatus.ENABLED)
                .max(Comparator.comparingInt(TemplateDef::getVersion));
    }
    @Override
    public Optional<TemplateDef> findTemplate(String id, int version) {
        List<TemplateDef> vs = templates.get(id);
        if (vs == null) return Optional.empty();
        if (version <= 0) return findTemplate(id);
        return vs.stream().filter(t -> t.getVersion() == version).findFirst();
    }
    @Override
    public List<TemplateDef> listTemplates(String keyword, int page, int size) {
        List<TemplateDef> out = new ArrayList<>();
        templates.forEach((id, vs) -> findTemplate(id).ifPresent(out::add));
        return out;
    }
    @Override public long countTemplates(String keyword) { return templates.size(); }
    @Override public List<TemplateDef> listTemplateVersions(String id) {
        return new ArrayList<>(templates.getOrDefault(id, List.of()));
    }
    @Override public void deleteTemplate(String id) { templates.remove(id); }
    @Override
    public void updateTemplateStatus(String id, int version, TemplateStatus status) {
        findTemplate(id, version).ifPresent(t -> t.setStatus(status));
    }

    @Override public void saveSubscription(SubscriptionDef d) { subs.put(d.getId(), d); }
    @Override public Optional<SubscriptionDef> findSubscription(String id) { return Optional.ofNullable(subs.get(id)); }
    @Override public List<SubscriptionDef> listSubscriptions(String serverId) { return new ArrayList<>(subs.values()); }
    @Override public void deleteSubscription(String id) { subs.remove(id); }
}
