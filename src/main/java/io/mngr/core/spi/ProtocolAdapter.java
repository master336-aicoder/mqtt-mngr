package io.mngr.core.spi;

import io.mngr.core.enums.Capability;
import io.mngr.core.enums.Protocol;
import io.mngr.core.model.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** 协议扩展点 SPI（NFR-8）。Adapter 不承担消息解析职责。 */
public interface ProtocolAdapter {

    Protocol protocol();

    /** 异步建连，结果经 ConnCallback 进入状态机。 */
    void connect(ServerConfig cfg, ConnCallback cb);

    void disconnect(String serverId, Duration drain);

    /** 管理面调用所需的凭据/参数变化后重建连接。 */
    default void reconnect(ServerConfig cfg, ConnCallback cb) {
        disconnect(cfg.getId(), Duration.ofSeconds(2));
        connect(cfg, cb);
    }

    ServerRuntime status(String serverId);

    CompletableFuture<PublishResult> publish(PublishRequest req, byte[] payload);

    /** 启动订阅，返回 handle（stopSubscription 用）。sink 在协议 IO 线程回调，须快速 handoff。 */
    String startSubscription(SubscriptionDef def, Consumer<RawIncoming> sink);

    void stopSubscription(String serverId, String handle);

    void pause(String subscriptionId);

    void resume(String subscriptionId);

    boolean supports(Capability cap);

    /** Kafka 提供；MQTT 空。 */
    java.util.Optional<TopicAdmin> topicAdmin(String serverId);

    /** 集群管理面（FR-2.4/2.5）；无凭据或能力不足时 empty。 */
    java.util.Optional<ClusterOps> clusterOps(String serverId);

    interface ConnCallback {
        void onConnected(String serverId, java.util.Map<String, Object> protocolInfo);
        void onDisconnected(String serverId, Throwable cause);
    }
}
