package io.mngr.store.h2.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("subscription")
public class SubscriptionEntity {
    @TableId(type = IdType.INPUT)
    private String id;
    private String serverId;
    private String topicId;
    private String topic;
    private String grp;
    private String state;
    private String optionsJson;
    private LocalDateTime createdAt;
}
