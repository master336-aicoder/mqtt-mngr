package io.mngr.core.spi;

import io.mngr.core.model.*;

import java.util.List;
import java.util.Optional;

/** 配置持久化 SPI：h2 / file 双实现。 */
public interface ConfigRepository {

    void saveServer(ServerConfig c);
    Optional<ServerConfig> findServer(String id);
    List<ServerConfig> listServers(String protocol, String keyword, int page, int size);
    long countServers(String protocol, String keyword);
    void deleteServer(String id);

    void saveTopic(TopicConfig c);
    Optional<TopicConfig> findTopic(String id);
    Optional<TopicConfig> findTopicByName(String serverId, String name);
    List<TopicConfig> listTopics(String serverId, String keyword, int page, int size);
    long countTopics(String serverId, String keyword);
    void deleteTopic(String id);

    /** 保存为新版本（version = max+1）。 */
    void saveTemplate(TemplateDef t);
    /** 当前启用版本。 */
    Optional<TemplateDef> findTemplate(String id);
    Optional<TemplateDef> findTemplate(String id, int version);
    List<TemplateDef> listTemplates(String keyword, int page, int size);
    long countTemplates(String keyword);
    List<TemplateDef> listTemplateVersions(String id);
    void deleteTemplate(String id);
    void updateTemplateStatus(String id, int version, io.mngr.core.enums.TemplateStatus status);

    void saveSubscription(SubscriptionDef d);
    Optional<SubscriptionDef> findSubscription(String id);
    List<SubscriptionDef> listSubscriptions(String serverId);
    void deleteSubscription(String id);
}
