package io.mngr.store.h2.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 复合主键 (id, version)：所有读写均经 QueryWrapper 双条件，不使用 selectById。 */
@Data
@TableName("msg_template")
public class TemplateEntity {
    private String id;
    private Integer version;
    private String name;
    private String description;
    private String status;
    private String fieldsJson;
    private String source;
    private String owner;
    private String tags;
    private Boolean deleted;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
