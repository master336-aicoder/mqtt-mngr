package io.mngr.store.h2.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.mngr.store.h2.entity.UserRoleEntity;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserRoleMapper extends BaseMapper<UserRoleEntity> {
    @Delete("DELETE FROM sys_user_role WHERE user_id = #{userId}")
    int deleteByUserId(Long userId);
}
