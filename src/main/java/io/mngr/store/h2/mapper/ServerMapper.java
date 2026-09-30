package io.mngr.store.h2.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.mngr.store.h2.entity.ServerEntity;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ServerMapper extends BaseMapper<ServerEntity> {}
