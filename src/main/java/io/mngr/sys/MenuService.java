package io.mngr.sys;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.store.h2.entity.MenuEntity;
import io.mngr.store.h2.mapper.MenuMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class MenuService {

    private final MenuMapper menuMapper;

    public List<MenuEntity> listAll() {
        return menuMapper.selectList(new QueryWrapper<MenuEntity>().orderByAsc("sort_order"));
    }

    public MenuEntity getById(Long id) {
        MenuEntity m = menuMapper.selectById(id);
        if (m == null) throw new MngrException(ErrorCode.NOT_FOUND, "菜单不存在: " + id);
        return m;
    }

    public MenuEntity save(MenuEntity in) {
        boolean isNew = in.getId() == null;
        if (isNew) {
            if (in.getMenuName() == null || in.getMenuName().isBlank()) {
                throw new MngrException(ErrorCode.VALIDATION, "菜单名称不能为空");
            }
            if (in.getParentId() == null) in.setParentId(0L);
            if (in.getMenuType() == null) in.setMenuType(1);
            if (in.getSortOrder() == null) in.setSortOrder(0);
            if (in.getStatus() == null) in.setStatus(1);
            in.setCreatedAt(LocalDateTime.now());
            menuMapper.insert(in);
        } else {
            MenuEntity cur = getById(in.getId());
            if (in.getParentId() != null) cur.setParentId(in.getParentId());
            if (in.getMenuName() != null) cur.setMenuName(in.getMenuName());
            if (in.getMenuType() != null) cur.setMenuType(in.getMenuType());
            if (in.getPath() != null) cur.setPath(in.getPath());
            if (in.getIcon() != null) cur.setIcon(in.getIcon());
            if (in.getPermCode() != null) cur.setPermCode(in.getPermCode());
            if (in.getSortOrder() != null) cur.setSortOrder(in.getSortOrder());
            if (in.getStatus() != null) cur.setStatus(in.getStatus());
            menuMapper.updateById(cur);
        }
        return in;
    }

    public void delete(Long id) {
        long children = menuMapper.selectCount(
                new QueryWrapper<MenuEntity>().eq("parent_id", id));
        if (children > 0) throw new MngrException(ErrorCode.VALIDATION, "存在子菜单，无法删除");
        menuMapper.deleteById(id);
    }
}
