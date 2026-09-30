package io.mngr.config;

import io.mngr.codec.json.JsonCodec;
import io.mngr.core.evt.EventBus;
import io.mngr.core.metrics.MngrMetrics;
import io.mngr.core.pub.PublisherEngine;
import io.mngr.core.server.ConnectionRegistry;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.spi.ProtocolAdapter;
import io.mngr.core.spi.RuntimeRepository;
import io.mngr.core.sub.SubscriberEngine;
import io.mngr.core.template.TemplateMatcher;
import io.mngr.core.template.TemplateService;
import io.mngr.core.topic.TopicManager;
import io.mngr.adapter.kafka.KafkaAdapter;
import io.mngr.adapter.mqtt.MqttAdapter;
import io.mngr.store.CredCipher;
import io.mngr.store.h2.H2ConfigRepository;
import io.mngr.store.h2.H2RuntimeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class CoreConfig {

    private final MngrProperties props;

    // ---------- h2 存储（默认模式；file 模式下完全不加载） ----------

    @Bean
    @ConditionalOnProperty(name = "mngr.store.mode", havingValue = "h2", matchIfMissing = true)
    public io.mngr.core.spi.ConfigRepository h2ConfigRepository(
            io.mngr.store.h2.mapper.ServerMapper sm, io.mngr.store.h2.mapper.TopicMapper tm,
            io.mngr.store.h2.mapper.TemplateMapper tpm, io.mngr.store.h2.mapper.SubscriptionMapper subm,
            CredCipher cipher) {
        return new H2ConfigRepository(sm, tm, tpm, subm, cipher);
    }

    @Bean
    @ConditionalOnProperty(name = "mngr.store.mode", havingValue = "h2", matchIfMissing = true)
    public io.mngr.core.spi.RuntimeRepository h2RuntimeRepository(JdbcTemplate jdbc) {
        return new H2RuntimeRepository(jdbc);
    }

    // ---------- 核心 Bean ----------

    @Bean public EventBus eventBus() { return new EventBus(); }
    @Bean public ConnectionRegistry connectionRegistry() { return new ConnectionRegistry(); }
    @Bean public MngrMetrics mngrMetrics(io.micrometer.core.instrument.MeterRegistry registry) {
        return new MngrMetrics(registry);
    }
    @Bean public JsonCodec jsonCodec() { return new JsonCodec(); }
    @Bean public KafkaAdapter kafkaAdapter() { return new KafkaAdapter(); }
    @Bean public MqttAdapter mqttAdapter() { return new MqttAdapter(); }

    @Bean
    public ServerManager serverManager(ConfigRepository repo, List<ProtocolAdapter> adapters,
                                       ConnectionRegistry registry, EventBus bus) {
        return new ServerManager(repo, adapters, registry, bus, props);
    }

    @Bean
    public TemplateMatcher templateMatcher(ConfigRepository repo) {
        return new TemplateMatcher(repo, TemplateMatcher.DEFAULT_THRESHOLD);
    }

    @Bean
    public TemplateService templateService(ConfigRepository repo, TemplateMatcher matcher) {
        return new TemplateService(repo, matcher);
    }

    @Bean
    public TopicManager topicManager(ConfigRepository repo) { return new TopicManager(repo); }

    @Bean
    public PublisherEngine publisherEngine(ConfigRepository repo, ServerManager sm, TopicManager tm,
                                           TemplateService ts, JsonCodec codec, MngrMetrics metrics,
                                           RuntimeRepository runtime, EventBus bus) {
        return new PublisherEngine(repo, sm, tm, ts, codec, metrics, runtime, props, bus);
    }

    @Bean
    public SubscriberEngine subscriberEngine(ConfigRepository repo, ServerManager sm, TopicManager tm,
                                             JsonCodec codec, TemplateMatcher matcher, MngrMetrics metrics,
                                             RuntimeRepository runtime, EventBus bus) {
        return new SubscriberEngine(repo, sm, tm, codec, matcher, metrics, runtime, props, bus);
    }

    /** 启动顺序：恢复配置 → EAGER 建连 → 恢复持久订阅；重连钩子挂接缓冲排空与订阅恢复。 */
    @Bean
    public StartupOrchestrator startupOrchestrator(ServerManager sm, SubscriberEngine se,
                                                   PublisherEngine pe, ConfigRepository repo) {
        return new StartupOrchestrator(sm, se, pe, repo);
    }

    @RequiredArgsConstructor
    public static class StartupOrchestrator {
        private final ServerManager sm;
        private final SubscriberEngine se;
        private final PublisherEngine pe;
        private final ConfigRepository repo;

        @EventListener(ApplicationReadyEvent.class)
        public void onReady() {
            sm.addReconnectHook(serverId -> {
                pe.drainBuffer(serverId);
                se.restoreFor(serverId);
            });
            sm.start();
            for (var d : repo.listSubscriptions(null)) {
                if (d.isPersistent() && "RUNNING".equals(d.getState())) {
                    try {
                        se.start(d.getId());
                    } catch (Exception e) {
                        log.warn("恢复订阅 {} 失败（将在重连后由钩子重试）: {}", d.getId(), e.toString());
                    }
                }
            }
        }
    }
}
