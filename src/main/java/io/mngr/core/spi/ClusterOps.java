package io.mngr.core.spi;

import java.util.List;
import java.util.Map;

/** 集群管理面（FR-2.4 Kafka K1~K18 / FR-2.5 EMQX E1~E14 的受控子集）。
 * 返回统一 Map 结构（协议中立的弱类型管理 DTO），字段命名对齐 REST 文档。 */
public interface ClusterOps {

    /** K1 / E1：集群概览（broker 列表或 EMQX 节点、版本）。 */
    Map<String, Object> overview();

    /** K2 / E5：topic 列表（含消息数/订阅者等扩展信息）。 */
    List<Map<String, Object>> listTopicsDetail();

    /** K3/K4：topic 详情与分区、lag。 */
    Map<String, Object> describeTopic(String topic);

    /** K5：生产 topic 消息量统计（endOffset 汇总）。 */
    Map<String, Object> topicStats(String topic);

    /** K8：消费组列表。 */
    List<Map<String, Object>> listGroups();

    /** K9：消费组详情（成员、assignment、committed、lag）。 */
    Map<String, Object> describeGroup(String groupId);

    /** K9x：重置 offset（latest|earliest|timestamp）。 */
    Map<String, Object> resetOffsets(String groupId, String topic, String strategy, Long timestamp);

    /** E3：客户端连接列表。 */
    List<Map<String, Object>> listClients(String filter, int limit);

    /** E6：订阅列表。 */
    List<Map<String, Object>> listSubscriptions(String filter, int limit);

    /** E2：EMQX 节点列表（Kafka 侧 empty）。 */
    List<Map<String, Object>> listNodes();

    /** E7：清除 retained 消息。 */
    Map<String, Object> clearRetained(String topic);

    /** 该 gateway 实际支持的操作集合（能力不足显式降级）。 */
    java.util.Set<String> supportedOperations();
}
