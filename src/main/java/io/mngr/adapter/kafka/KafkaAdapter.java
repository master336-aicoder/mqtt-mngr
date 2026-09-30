package io.mngr.adapter.kafka;

import io.mngr.core.enums.*;
import io.mngr.core.exception.PublishException;
import io.mngr.core.model.*;
import io.mngr.core.spi.ClusterOps;
import io.mngr.core.spi.ProtocolAdapter;
import io.mngr.core.spi.TopicAdmin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Kafka Adapter（kafka-clients 直用，D2）。 */
@Slf4j
public class KafkaAdapter implements ProtocolAdapter {

    static class Holder {
        KafkaProducer<byte[], byte[]> producer;
        AdminClient admin;
        ServerConfig cfg;
        final Map<String, PollTask> tasks = new ConcurrentHashMap<>();
    }

    final Map<String, Holder> holders = new ConcurrentHashMap<>();
    private final ExecutorService connExec = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "mngr-kafka-conn");
        t.setDaemon(true);
        return t;
    });

    @Override
    public Protocol protocol() { return Protocol.KAFKA; }

    @Override
    public void connect(ServerConfig cfg, ConnCallback cb) {
        connExec.submit(() -> {
            try {
                Properties p = new Properties();
                p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.connStr("bootstrapServers", ""));
                p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
                p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
                p.put(ProducerConfig.ACKS_CONFIG, cfg.connStr("acks", "all"));
                p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, cfg.connBool("idempotence", true));
                p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, cfg.connInt("maxInFlight", 5));
                p.put(ProducerConfig.LINGER_MS_CONFIG, cfg.connInt("lingerMs", 5));
                p.put(ProducerConfig.BATCH_SIZE_CONFIG, cfg.connInt("batchSize", 65536));
                p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, cfg.connInt("sendTimeoutMs", 10_000));
                if (cfg.getConn().containsKey("client.id") || cfg.getConn().containsKey("clientId")) {
                    p.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.connStr("clientId", "mngr"));
                }
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p);

                Properties ap = new Properties();
                ap.put("bootstrap.servers", cfg.connStr("bootstrapServers", ""));
                ap.put("client.id", cfg.connStr("clientId", "mngr-admin"));
                ap.put("request.timeout.ms", 5000);
                ap.put("default.api.timeout.ms", 8000);
                AdminClient admin = AdminClient.create(ap);

                // 建连验证：拉一次元数据
                Collection<Node> nodes = admin.describeCluster().nodes().get();
                String clusterId = admin.describeCluster().clusterId().get();
                Node controller = admin.describeCluster().controller().get();

                Holder old = holders.get(cfg.getId());
                if (old != null) closeHolder(old);
                Holder h = new Holder();
                h.producer = producer;
                h.admin = admin;
                h.cfg = cfg;
                holders.put(cfg.getId(), h);
                // producer 创建是懒连接，元数据成功后视为可用
                cb.onConnected(cfg.getId(), Map.of(
                        "clusterId", clusterId,
                        "nodes", nodes.size(),
                        "controller", controller.host() + ":" + controller.port()));
            } catch (Throwable e) {
                cb.onDisconnected(cfg.getId(), e);
            }
        });
    }

    private void closeHolder(Holder h) {
        h.tasks.values().forEach(PollTask::stop);
        try { h.producer.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
        try { h.admin.close(Duration.ofSeconds(2)); } catch (Exception ignored) { }
    }

    @Override
    public void disconnect(String serverId, Duration drain) {
        Holder h = holders.remove(serverId);
        if (h != null) {
            try { h.producer.flush(); } catch (Exception ignored) { }
            closeHolder(h);
        }
    }

    @Override
    public ServerRuntime status(String serverId) {
        // 状态真相在 ConnectionRegistry；adapter 仅在 publish 时校验句柄。
        return ServerRuntime.builder().serverId(serverId)
                .state(holders.containsKey(serverId) ? ServerState.CONNECTED : ServerState.INIT).build();
    }

    Holder require(String serverId) {
        Holder h = holders.get(serverId);
        if (h == null) throw PublishException.of(FailKind.DISCONNECTED, ErrorCode.NOT_CONNECTED, "kafka client 未建连");
        return h;
    }

    @Override
    public CompletableFuture<PublishResult> publish(PublishRequest req, byte[] payload) {
        Holder h = require(req.getServerId());
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(
                req.getTopic(),
                req.getKey() == null ? null : req.getKey().getBytes(StandardCharsets.UTF_8),
                payload);
        req.headersOrNew().forEach((k, v) ->
                rec.headers().add(k, v == null ? new byte[0] : v.getBytes(StandardCharsets.UTF_8)));
        long ts = System.currentTimeMillis();
        CompletableFuture<PublishResult> f = new CompletableFuture<>();
        h.producer.send(rec, (meta, e) -> {
            if (e != null) f.completeExceptionally(classify(e));
            else f.complete(PublishResult.builder().success(true)
                    .serverId(req.getServerId()).topic(req.getTopic())
                    .messageId(req.getHeaders().get("x-msg-id"))
                    .protocolMeta(Map.of(
                            "partition", meta == null ? -1 : meta.partition(),
                            "offset", meta == null ? -1 : meta.offset(),
                            "timestamp", ts))
                    .build());
        });
        return f.orTimeout(15, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    PublishException pe = ex instanceof PublishException p
                            ? p : classify(ex instanceof CompletionException c && c.getCause() instanceof Exception ce ? ce : (Throwable) ex);
                    return PublishResult.builder().success(false)
                            .serverId(req.getServerId()).topic(req.getTopic())
                            .errorCode(pe.getErrorCode().code())
                            .errorMessage(pe.getMessage()).build();
                });
    }

    static FailKind kindOf(Throwable e) {
        Throwable t = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (t instanceof RetriableException) return FailKind.RETRYABLE;
        if (t instanceof java.util.concurrent.TimeoutException) return FailKind.TIMEOUT;
        if (t instanceof NotLeaderForPartitionException || t instanceof LeaderNotAvailableException
                || t instanceof UnknownTopicOrPartitionException || t instanceof NetworkException) {
            return FailKind.RETRYABLE;
        }
        if (t instanceof DisconnectException) return FailKind.DISCONNECTED;
        return FailKind.NON_RETRYABLE;
    }

    static PublishException classify(Throwable e) {
        FailKind k = kindOf(e);
        ErrorCode ec = e instanceof RecordTooLargeException ? ErrorCode.PACKET_TOO_LARGE
                : e instanceof TopicAuthorizationException ? ErrorCode.PUBLISH_FAILED
                : ErrorCode.PUBLISH_FAILED;
        return new PublishException(k, ec, String.valueOf(e.getMessage()), e);
    }

    @Override
    public String startSubscription(SubscriptionDef def, Consumer<RawIncoming> sink) {
        Holder h = require(def.getServerId());
        String handle = "ksub-" + def.getId();
        PollTask task = new PollTask(h, def, sink);
        h.tasks.put(handle, task);
        task.start();
        return handle;
    }

    @Override
    public void stopSubscription(String serverId, String handle) {
        Holder h = holders.get(serverId);
        if (h != null) {
            PollTask t = h.tasks.remove(handle);
            if (t != null) t.stop();
        }
    }

    @Override
    public void pause(String subscriptionId) {
        forEachTask(t -> { if (t.def.getId().equals(subscriptionId)) t.paused = true; });
    }

    @Override
    public void resume(String subscriptionId) {
        forEachTask(t -> { if (t.def.getId().equals(subscriptionId)) t.paused = false; });
    }

    private void forEachTask(Consumer<PollTask> c) {
        holders.values().forEach(h -> h.tasks.values().forEach(c));
    }

    @Override
    public boolean supports(Capability cap) {
        return switch (cap) {
            case OFFSET_REWIND, TOPIC_ADMIN, ORDERED_KEY, CLUSTER_OPS, SHARED_SUBSCRIPTION -> true;
            default -> false;
        };
    }

    @Override
    public Optional<TopicAdmin> topicAdmin(String serverId) {
        Holder h = holders.get(serverId);
        return h == null ? Optional.empty() : Optional.of(new KafkaTopicAdmin(h));
    }

    @Override
    public Optional<ClusterOps> clusterOps(String serverId) {
        Holder h = holders.get(serverId);
        return h == null ? Optional.empty() : Optional.of(new KafkaClusterOps(h));
    }

    // ---------------- consumer poll task ----------------

    class PollTask {
        private final Holder holder;
        final SubscriptionDef def;
        private final Consumer<RawIncoming> sink;
        private KafkaConsumer<byte[], byte[]> consumer;
        private Thread thread;
        volatile boolean paused;
        private volatile boolean stopped;

        PollTask(Holder holder, SubscriptionDef def, Consumer<RawIncoming> sink) {
            this.holder = holder;
            this.def = def;
            this.sink = sink;
        }

        void start() {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, holder.cfg.connStr("bootstrapServers", ""));
            p.put(ConsumerConfig.GROUP_ID_CONFIG,
                    def.getGroup() == null || def.getGroup().isBlank() ? "mngr-" + def.getId() : def.getGroup());
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    "EARLIEST".equals(def.getOffsetStrategy()) ? "earliest" : "latest");
            p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200);
            consumer = new KafkaConsumer<>(p);
            thread = new Thread(this::loop, "mngr-kafka-poll-" + def.getId());
            thread.setDaemon(true);
            thread.start();
        }

        private void loop() {
            try {
                Collection<TopicPartition> assigned = new ArrayList<>();
                consumer.subscribe(List.of(def.getTopic()), new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> parts) { }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                        assigned.addAll(parts);
                        try {
                            if ("EARLIEST".equals(def.getOffsetStrategy())) {
                                consumer.seekToBeginning(parts);
                            } else if ("TIMESTAMP".equals(def.getOffsetStrategy()) && def.getOffsetTimestamp() != null) {
                                Map<TopicPartition, Long> q = new HashMap<>();
                                parts.forEach(tp -> q.put(tp, def.getOffsetTimestamp()));
                                consumer.offsetsForTimes(q).forEach((tp, om) -> {
                                    if (om != null) consumer.seek(tp, om.offset());
                                });
                            }
                        } catch (Exception e) {
                            log.warn("seek failed sub={}: {}", def.getId(), e.toString());
                        }
                    }
                });
                long lastCommit = 0;
                while (!stopped) {
                    if (paused) {
                        Thread.sleep(200);
                        continue;
                    }
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));
                    for (ConsumerRecord<byte[], byte[]> r : records) {
                        Map<String, String> headers = new HashMap<>();
                        for (Header hd : r.headers()) headers.put(hd.key(), new String(hd.value(), StandardCharsets.UTF_8));
                        Map<String, Object> meta = new HashMap<>();
                        meta.put("partition", r.partition());
                        meta.put("offset", r.offset());
                        meta.put("kafkaTs", r.timestamp());
                        sink.accept(RawIncoming.builder()
                                .serverId(holder.cfg.getId()).subscriptionId(def.getId())
                                .topic(r.topic()).payload(r.value())
                                .headers(headers).protocolMeta(meta)
                                .receiveTs(System.currentTimeMillis()).build());
                    }
                    if (!records.isEmpty() && System.currentTimeMillis() - lastCommit > 500) {
                        consumer.commitAsync();
                        lastCommit = System.currentTimeMillis();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (org.apache.kafka.common.errors.WakeupException we) {
                // stop()
            } catch (Exception e) {
                log.warn("kafka poll loop end sub={} err={}", def.getId(), e.toString());
            } finally {
                try { consumer.commitSync(); } catch (Exception ignored) { }
                try { consumer.close(Duration.ofSeconds(3)); } catch (Exception ignored) { }
            }
        }

        void stop() {
            stopped = true;
            if (thread != null) thread.interrupt();
        }
    }

    // ---------------- TopicAdmin ----------------

    static class KafkaTopicAdmin implements TopicAdmin {
        private final Holder h;
        KafkaTopicAdmin(Holder h) { this.h = h; }

        @Override
        public List<String> listTopics() {
            try {
                return new ArrayList<>(h.admin.listTopics().names().get());
            } catch (Exception e) {
                throw new IllegalStateException("listTopics: " + e.getMessage(), e);
            }
        }

        @Override
        public void createTopic(String name, int partitions, short rf, Map<String, String> config) {
            try {
                h.admin.createTopics(List.of(new NewTopic(name, partitions, rf)
                        .configs(config == null ? Map.of() : config)))
                        .all().get();
            } catch (Exception e) {
                throw new IllegalStateException("createTopic: " + rootMsg(e), e);
            }
        }

        @Override
        public void deleteTopic(String name) {
            try {
                h.admin.deleteTopics(List.of(name)).all().get();
            } catch (Exception e) {
                throw new IllegalStateException("deleteTopic: " + rootMsg(e), e);
            }
        }

        @Override
        public void addPartitions(String name, int total) {
            try {
                h.admin.createPartitions(Map.of(name, NewPartitions.increaseTo(total))).all().get();
            } catch (Exception e) {
                throw new IllegalStateException("addPartitions: " + rootMsg(e), e);
            }
        }

        @Override
        public Map<String, Object> describe(String name) {
            try {
                TopicDescription td = h.admin.describeTopics(List.of(name)).values().get(name).get();
                List<Map<String, Object>> parts = new ArrayList<>();
                for (TopicPartitionInfo pi : td.partitions()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("partition", pi.partition());
                    m.put("leader", pi.leader() == null ? null : pi.leader().id());
                    m.put("replicas", pi.replicas().stream().map(Node::id).toList());
                    m.put("inSyncReplicas", pi.isr().stream().map(Node::id).toList());
                    parts.add(m);
                }
                List<TopicPartition> tps = td.partitions().stream()
                        .map(pi -> new TopicPartition(name, pi.partition())).toList();
                Map<TopicPartition, Long> end = h.admin.listOffsets(
                        tps.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
                        .all().get().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                            try { return e.getValue().offset(); } catch (Exception ex) { return -1L; }
                        }));
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("topic", name);
                out.put("internal", td.isInternal());
                out.put("partitions", parts);
                out.put("endOffsets", end.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().partition(), Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new)));
                return out;
            } catch (Exception e) {
                throw new IllegalStateException("describe: " + rootMsg(e), e);
            }
        }

        @Override
        public Set<String> capabilities() {
            return Set.of("LIST", "CREATE", "DELETE", "ADD_PARTITIONS", "DESCRIBE", "LAG");
        }
    }

    static String rootMsg(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) t = t.getCause();
        return t.getMessage();
    }
}
