package io.mngr.store.h2.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("sys_user_role")
public class UserRoleEntity {
    private Long userId;
    private Long roleId;
}
