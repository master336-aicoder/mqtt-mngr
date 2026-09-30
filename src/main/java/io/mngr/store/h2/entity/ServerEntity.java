package io.mngr.store.h2.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("server_config")
public class ServerEntity {
    @TableId(type = IdType.INPUT)
    private String id;
    private String name;
    private String protocol;
    private Boolean enabled;
    private String connMode;
    private String connJson;
    private byte[] credEnc;
    private String mgmtJson;
    private String tags;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
