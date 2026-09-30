package io.mngr.core.enums;

/** 能力声明（NFR-8：不支持的能力显式暴露而非静默降级）。 */
public enum Capability {
    OFFSET_REWIND,      // Kafka: seek/reset offsets
    TOPIC_ADMIN,        // Kafka: create/delete topics
    SHARED_SUBSCRIPTION,// MQTT: $share/group/topic; Kafka: consumer group
    RETAIN,             // MQTT retained
    QOS,                // MQTT QoS
    ORDERED_KEY,        // Kafka partition key ordering
    MESSAGE_EXPIRY,     // MQTT5 messageExpiryInterval
    SUBSCRIPTION_ID,    // MQTT5 subscription identifier
    RPC_REQUEST,        // MQTT5 request/response
    CLUSTER_OPS         // 集群管理面
}
