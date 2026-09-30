package io.mngr.core.spi;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Topic 管理面（Kafka 专属；MQTT adapter 不实现）。 */
public interface TopicAdmin {
    List<String> listTopics();

    void createTopic(String name, int partitions, short replicationFactor, Map<String, String> config);

    void deleteTopic(String name);

    void addPartitions(String name, int totalPartitions);

    Map<String, Object> describe(String name);   // 分区/leader/LEO/lag

    Set<String> capabilities();
}
