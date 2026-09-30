package io.mngr.store.h2.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("topic_config")
public class TopicEntity {
    @TableId(type = IdType.INPUT)
    private String id;
    private String serverId;
    private String namePattern;
    private String direction;
    private String format;
    private Integer qos;
    private String keyStrategy;
    private String templateId;
    private Boolean autoMatch;
    private String tags;
    private String owner;
    private String remark;
    private Boolean deleted;
    private LocalDateTime updatedAt;
}
