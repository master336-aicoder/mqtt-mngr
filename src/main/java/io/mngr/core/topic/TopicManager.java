package io.mngr.core.topic;

import io.mngr.core.enums.Direction;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.SubscriptionDef;
import io.mngr.core.model.TopicConfig;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.topic.WildcardMatcher;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.Optional;

@RequiredArgsConstructor
public class TopicManager {

    private final ConfigRepository repo;

    public TopicConfig save(TopicConfig c) {
        if (c.getServerId() == null || repo.findServer(c.getServerId()).isEmpty()) {
            throw new MngrException(ErrorCode.NOT_FOUND, "server 不存在: " + c.getServerId());
        }
        if (c.getNamePattern() == null || c.getNamePattern().isBlank()) {
            throw new MngrException(ErrorCode.VALIDATION, "topic 名称/模式必填");
        }
        if (c.getId() == null || c.getId().isBlank()) {
            // 同 server 同名模式视为更新
            Optional<TopicConfig> dup = repo.findTopicByName(c.getServerId(), c.getNamePattern());
            c.setId(dup.map(TopicConfig::getId).orElse("tpc-" + java.util.UUID.randomUUID().toString().substring(0, 8)));
        } else if (c.getDirection() == null) {
            c.setDirection(Direction.BOTH);
        }
        c.setUpdatedAt(Instant.now());
        repo.saveTopic(c);
        return c;
    }

    /** 软删除：活跃订阅引用检查（FR-2.1.4）。 */
    public void delete(String id, boolean force) {
        TopicConfig c = repo.findTopic(id).orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND));
        if (!force) {
            for (SubscriptionDef s : repo.listSubscriptions(c.getServerId())) {
                if (id.equals(s.getTopicId())) {
                    throw new MngrException(ErrorCode.IN_USE, "topic 仍被订阅引用: " + s.getId());
                }
            }
        }
        repo.deleteTopic(id);
    }

    /** 解析发布目标：优先请求 topic，否则按 topicId 引用；方向校验。 */
    public String resolvePublishTopic(String serverId, String topicId, String topic) {
        if (topicId != null && !topicId.isBlank()) {
            TopicConfig c = repo.findTopic(topicId)
                    .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND, "topic 引用不存在: " + topicId));
            if (c.getDirection() == Direction.SUB) {
                throw new MngrException(ErrorCode.VALIDATION, "topic " + c.getNamePattern() + " 方向为 SUB，禁止发布");
            }
            if (topic == null || topic.isBlank()) return c.getNamePattern();
        }
        if (topic == null || topic.isBlank()) {
            throw new MngrException(ErrorCode.VALIDATION, "topic 与 topicId 至少填一");
        }
        // 若命中登记模式则做方向检查
        for (TopicConfig c : repo.listTopics(serverId, null, 0, 10000)) {
            if (WildcardMatcher.matches(c.getNamePattern(), topic) && c.getDirection() == Direction.SUB) {
                throw new MngrException(ErrorCode.VALIDATION, "topic 命中登记模式 " + c.getNamePattern() + "（SUB），禁止发布");
            }
        }
        return topic;
    }

    /** 订阅目标解析：topicId 引用模式/名。 */
    public String resolveSubscribeTopic(String serverId, String topicId, String topic) {
        if (topic != null && !topic.isBlank()) return topic;
        TopicConfig c = repo.findTopic(topicId)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND, "topic 引用不存在: " + topicId));
        if (c.getDirection() == Direction.PUB) {
            throw new MngrException(ErrorCode.VALIDATION, "topic " + c.getNamePattern() + " 方向为 PUB，禁止订阅");
        }
        return c.getNamePattern();
    }
}
