package io.mngr.store.h2.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.mngr.store.h2.entity.UserEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface UserMapper extends BaseMapper<UserEntity> {
    @Select("SELECT r.role_code FROM sys_role r INNER JOIN sys_user_role ur ON r.id = ur.role_id WHERE ur.user_id = #{userId}")
    java.util.List<String> selectRoleCodesByUserId(Long userId);

    @Select("SELECT r.id, r.role_code, r.role_name FROM sys_role r INNER JOIN sys_user_role ur ON r.id = ur.role_id WHERE ur.user_id = #{userId}")
    java.util.List<java.util.Map<String, Object>> selectRolesByUserId(Long userId);
}
