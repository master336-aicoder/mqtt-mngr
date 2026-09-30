package io.mngr.adapter.kafka;

import io.mngr.core.spi.ClusterOps;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** K1~K9/K17 管理面（AdminClient 直映射，超时 5s）。 */
public class KafkaClusterOps implements ClusterOps {

    private final KafkaAdapter.Holder h;

    KafkaClusterOps(KafkaAdapter.Holder h) { this.h = h; }

    private <T> T get(java.util.concurrent.Future<T> f) {
        try {
            return f.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(KafkaAdapter.rootMsg(e), e);
        }
    }

    @Override
    public Map<String, Object> overview() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("protocol", "KAFKA");
        out.put("clusterId", get(h.admin.describeCluster().clusterId()));
        Node controller = get(h.admin.describeCluster().controller());
        out.put("controller", controller.host() + ":" + controller.port());
        out.put("nodes", get(h.admin.describeCluster().nodes()).stream()
                .map(n -> Map.of("id", n.id(), "host", n.host(), "port", n.port())).toList());
        Set<String> topics = get(h.admin.listTopics().names());
        out.put("topicCount", topics.size());
        out.put("consumerGroupCount", get(h.admin.listConsumerGroups().all()).size());
        return out;
    }

    @Override
    public List<Map<String, Object>> listTopicsDetail() {
        Set<String> names = get(h.admin.listTopics().names());
        Map<String, TopicDescription> descs = get(h.admin.describeTopics(new ArrayList<>(names)).allTopicNames());
        List<Map<String, Object>> out = new ArrayList<>();
        descs.forEach((name, td) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("topic", name);
            m.put("partitions", td.partitions().size());
            m.put("replicationFactor", td.partitions().isEmpty() ? null : td.partitions().get(0).replicas().size());
            m.put("internal", td.isInternal());
            out.add(m);
        });
        out.sort(Comparator.comparing(m -> String.valueOf(m.get("topic"))));
        return out;
    }

    @Override
    public Map<String, Object> describeTopic(String topic) {
        TopicDescription td = get(h.admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        out.put("internal", td.isInternal());
        out.put("partitions", td.partitions().stream().map(pi -> {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("partition", pi.partition());
            p.put("leader", pi.leader() == null ? null : pi.leader().id());
            p.put("replicas", pi.replicas().stream().map(Node::id).toList());
            p.put("isr", pi.isr().stream().map(Node::id).toList());
            return p;
        }).toList());
        try {
            out.put("config", describeConfig(topic));
        } catch (Exception ignored) { }
        return out;
    }

    Map<String, String> describeConfig(String topic) {
        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config c = get(h.admin.describeConfigs(List.of(cr)).all()).get(cr);
        Map<String, String> out = new TreeMap<>();
        c.entries().forEach(e -> out.put(e.name(), e.value()));
        return out;
    }

    @Override
    public Map<String, Object> topicStats(String topic) {
        TopicDescription td = get(h.admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        List<TopicPartition> tps = td.partitions().stream()
                .map(pi -> new TopicPartition(topic, pi.partition())).toList();
        Map<TopicPartition, Long> ends = partitionOffsets(
                h.admin.listOffsets(tps.stream().collect(Collectors.toMap(tp -> tp, tp ->
                        OffsetSpec.latest()))));
        long totalEnd = ends.values().stream().mapToLong(Long::longValue).sum();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        out.put("endOffsets", ends.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey().partition(), Map.Entry::getValue, (a, b) -> a, TreeMap::new)));
        out.put("totalMessages", totalEnd);
        out.put("partitionCount", tps.size());
        return out;
    }

    static Map<TopicPartition, Long> partitionOffsets(ListOffsetsResult r) {
        Map<TopicPartition, Long> out = new LinkedHashMap<>();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> all;
        try {
            all = r.all().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(KafkaAdapter.rootMsg(e), e);
        }
        all.forEach((tp, info) -> out.put(tp, info == null ? -1L : info.offset()));
        return out;
    }

    @Override
    public List<Map<String, Object>> listGroups() {
        return get(h.admin.listConsumerGroups().all()).stream().map(dg -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("groupId", dg.groupId());
            m.put("state", dg.state().toString());
            m.put("simpleConsumerGroup", dg.isSimpleConsumerGroup());
            return m;
        }).sorted(Comparator.comparing(m -> String.valueOf(m.get("groupId")))).toList();
    }

    @Override
    public Map<String, Object> describeGroup(String groupId) {
        Map<String, Object> out = new LinkedHashMap<>();
        ConsumerGroupDescription gd = get(h.admin.describeConsumerGroups(List.of(groupId)).all()).get(groupId);
        out.put("groupId", gd.groupId());
        out.put("state", gd.state().toString());
        out.put("partitionAssignor", gd.partitionAssignor());
        out.put("members", gd.members().stream().map(cm -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("memberId", cm.consumerId());
            m.put("clientId", cm.clientId());
            m.put("host", cm.host());
            m.put("assignments", cm.assignment().topicPartitions().stream()
                    .map(tp -> tp.topic() + ":" + tp.partition()).sorted().toList());
            return m;
        }).toList());
        Map<String, Object> committed = new LinkedHashMap<>();
        Map<String, Long> lagByTopic = new LinkedHashMap<>();
        try {
            Map<TopicPartition, OffsetAndMetadata> offs =
                    get(h.admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
            List<String> topics = offs.keySet().stream().map(TopicPartition::topic).distinct().toList();
            Map<TopicPartition, Long> ends = topics.isEmpty() ? Map.of() : endOffsetsFor(topics);
            offs.forEach((tp, om) -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("topic", tp.topic());
                row.put("partition", tp.partition());
                row.put("committedOffset", om.offset());
                Long end = ends.get(tp);
                if (end != null) {
                    long lag = Math.max(0, end - om.offset());
                    row.put("endOffset", end);
                    row.put("lag", lag);
                    lagByTopic.merge(tp.topic(), lag, Long::sum);
                }
                committed.put(tp.topic() + ":" + tp.partition(), row);
            });
        } catch (Exception e) {
            out.put("offsetsError", KafkaAdapter.rootMsg(e));
        }
        out.put("offsets", committed);
        out.put("totalLag", lagByTopic.values().stream().mapToLong(Long::longValue).sum());
        out.put("lagByTopic", lagByTopic);
        return out;
    }

    Map<TopicPartition, Long> endOffsetsFor(List<String> topics) {
        Map<TopicPartition, OffsetSpec> q = new LinkedHashMap<>();
        for (String t : topics) {
            try {
                TopicDescription td = get(h.admin.describeTopics(List.of(t)).allTopicNames()).get(t);
                for (TopicPartitionInfo pi : td.partitions()) q.put(new TopicPartition(t, pi.partition()),
                        OffsetSpec.latest());
            } catch (Exception ignored) { }
        }
        return q.isEmpty() ? Map.of() : partitionOffsets(h.admin.listOffsets(q));
    }

    @Override
    public Map<String, Object> resetOffsets(String groupId, String topic, String strategy, Long timestamp) {
        // 通过删除已提交 offset 实现 earliest/latest 重置（活跃消费组由重平衡生效）
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            Map<TopicPartition, OffsetAndMetadata> offs =
                    get(h.admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
            List<TopicPartition> tps = offs.keySet().stream().filter(tp -> tp.topic().equals(topic)).toList();
            if (tps.isEmpty()) {
                TopicDescription td = get(h.admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
                tps = td.partitions().stream().map(pi -> new TopicPartition(topic, pi.partition())).toList();
            }
            if ("LATEST".equalsIgnoreCase(strategy)) {
                Map<TopicPartition, Long> ends = partitionOffsets(h.admin.listOffsets(
                        tps.stream().collect(Collectors.toMap(tp -> tp, tp ->
                                OffsetSpec.latest()))));
                Map<TopicPartition, OffsetAndMetadata> newOffs = ends.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
                get(h.admin.alterConsumerGroupOffsets(groupId, newOffs).all());
            } else if ("EARLIEST".equalsIgnoreCase(strategy)) {
                get(h.admin.deleteConsumerGroupOffsets(groupId, new HashSet<>(tps)).all());
            } else {
                throw new IllegalArgumentException("本期支持 LATEST/EARLIEST，TIMESTAMP 需消费者在线执行");
            }
            out.put("ok", true);
            out.put("groupId", groupId);
            out.put("topic", topic);
            out.put("strategy", strategy);
            out.put("partitions", tps.size());
        } catch (Exception e) {
            out.put("ok", false);
            out.put("error", KafkaAdapter.rootMsg(e));
        }
        return out;
    }

    @Override
    public List<Map<String, Object>> listClients(String filter, int limit) {
        // Kafka 无客户端连接枚举 API → 以消费组+生产者近似
        return List.of();
    }

    @Override
    public List<Map<String, Object>> listSubscriptions(String filter, int limit) {
        return get(h.admin.listConsumerGroups().all()).stream().map(dg -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("groupId", dg.groupId());
            m.put("state", dg.state().toString());
            return m;
        }).toList();
    }

    @Override
    public List<Map<String, Object>> listNodes() {
        return get(h.admin.describeCluster().nodes()).stream()
                .map(n -> Map.<String, Object>of("id", n.id(), "host", n.host(), "port", n.port())).toList();
    }

    @Override
    public Map<String, Object> clearRetained(String topic) {
        throw new UnsupportedOperationException("Kafka 无 retained 概念");
    }

    @Override
    public Set<String> supportedOperations() {
        return Set.of("overview", "listTopicsDetail", "describeTopic", "topicStats",
                "listGroups", "describeGroup", "resetOffsets", "listNodes");
    }
}
