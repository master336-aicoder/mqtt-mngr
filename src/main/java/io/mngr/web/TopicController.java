package io.mngr.web;

import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.Page;
import io.mngr.core.model.ServerConfig;
import io.mngr.core.model.TopicConfig;
import io.mngr.core.server.ServerManager;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.spi.TopicAdmin;
import io.mngr.core.topic.TopicManager;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/topics")
@RequiredArgsConstructor
public class TopicController {

    private final ConfigRepository repo;
    private final TopicManager topicManager;
    private final ServerManager serverManager;
    private final AuditHelper audit;

    @GetMapping
    public ApiResponse<Page<Map<String, Object>>> list(@RequestParam(required = false) String serverId,
                                                       @RequestParam(required = false) String keyword,
                                                       @RequestParam(defaultValue = "0") int page,
                                                       @RequestParam(defaultValue = "20") int size) {
        List<TopicConfig> items = repo.listTopics(serverId, keyword, page, size);
        long total = repo.countTopics(serverId, keyword);
        return ApiResponse.ok(new Page<>(items.stream().map(this::view).toList(), total));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable String id) {
        return ApiResponse.ok(view(repo.findTopic(id)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND))));
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> save(@RequestBody TopicConfig in) {
        TopicConfig before = in.getId() == null ? null : repo.findTopic(in.getId()).orElse(null);
        TopicConfig saved = topicManager.save(in);
        audit.record(before == null ? "CREATE" : "UPDATE", "TOPIC", saved.getId(), before, saved);
        return ApiResponse.ok(view(saved));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable String id,
                                    @RequestParam(defaultValue = "false") boolean force) {
        TopicConfig c = repo.findTopic(id).orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
        topicManager.delete(id, force);
        audit.record("DELETE", "TOPIC", id, c, null);
        return ApiResponse.ok(null);
    }

    /** Topic 浏览（FR-2.2.2）：Kafka 实时来自 broker；MQTT 本地登记 + 通配说明。 */
    @GetMapping("/browse")
    public ApiResponse<Map<String, Object>> browse(@RequestParam String serverId) {
        ServerConfig cfg = repo.findServer(serverId)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND, "server 不存在"));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("protocol", cfg.getProtocol().name());
        out.put("localTopics", repo.listTopics(serverId, null, 0, 1000).stream().map(this::view).toList());
        if (cfg.getProtocol() == io.mngr.core.enums.Protocol.KAFKA) {
            try {
                TopicAdmin ta = serverManager.adapter(cfg.getProtocol()).topicAdmin(serverId)
                        .orElseThrow(() -> new MngrException(ErrorCode.NOT_CONNECTED, "未建连"));
                out.put("brokerTopics", ta.listTopics());
                out.put("brokerLive", true);
            } catch (Exception e) {
                out.put("brokerTopics", List.of());
                out.put("brokerLive", false);
                out.put("brokerError", e.getMessage());
            }
        } else {
            out.put("brokerNote", "MQTT 无 topic 创建概念，展示本地登记与通配模式");
            out.put("brokerTopics", List.of());
            out.put("brokerLive", false);
        }
        return ApiResponse.ok(out);
    }

    Map<String, Object> view(TopicConfig c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", c.getId());
        m.put("serverId", c.getServerId());
        m.put("namePattern", c.getNamePattern());
        m.put("direction", c.getDirection().name());
        m.put("format", c.getFormat());
        m.put("qos", c.getQos());
        m.put("keyStrategy", c.getKeyStrategy());
        m.put("templateId", c.getTemplateId());
        m.put("autoMatch", c.isAutoMatch());
        m.put("tags", c.getTags());
        m.put("owner", c.getOwner());
        m.put("remark", c.getRemark());
        m.put("updatedAt", c.getUpdatedAt());
        return m;
    }
}
