package io.mngr.sys;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.store.h2.entity.RoleEntity;
import io.mngr.store.h2.entity.RoleMenuEntity;
import io.mngr.store.h2.mapper.RoleMapper;
import io.mngr.store.h2.mapper.RoleMenuMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RoleService {

    private final RoleMapper roleMapper;
    private final RoleMenuMapper roleMenuMapper;

    public List<RoleEntity> list(String keyword, int page, int size) {
        QueryWrapper<RoleEntity> w = new QueryWrapper<>();
        if (keyword != null && !keyword.isBlank()) {
            w.like("role_name", keyword).or().like("role_code", keyword);
        }
        w.orderByAsc("id").last("LIMIT " + size + " OFFSET " + (long) page * size);
        return roleMapper.selectList(w);
    }

    public long count(String keyword) {
        QueryWrapper<RoleEntity> w = new QueryWrapper<>();
        if (keyword != null && !keyword.isBlank()) {
            w.like("role_name", keyword).or().like("role_code", keyword);
        }
        return roleMapper.selectCount(w);
    }

    public RoleEntity getById(Long id) {
        RoleEntity r = roleMapper.selectById(id);
        if (r == null) throw new MngrException(ErrorCode.NOT_FOUND, "角色不存在: " + id);
        return r;
    }

    @Transactional
    public RoleEntity save(RoleEntity in) {
        boolean isNew = in.getId() == null;
        if (isNew) {
            if (in.getRoleCode() == null || in.getRoleCode().isBlank()) {
                throw new MngrException(ErrorCode.VALIDATION, "角色编码不能为空");
            }
            RoleEntity existing = roleMapper.selectOne(
                    new QueryWrapper<RoleEntity>().eq("role_code", in.getRoleCode()));
            if (existing != null) {
                throw new MngrException(ErrorCode.DUP_NAME, "角色编码已存在: " + in.getRoleCode());
            }
            if (in.getStatus() == null) in.setStatus(1);
            in.setCreatedAt(LocalDateTime.now());
            in.setUpdatedAt(LocalDateTime.now());
            roleMapper.insert(in);
        } else {
            RoleEntity cur = getById(in.getId());
            if (in.getRoleName() != null) cur.setRoleName(in.getRoleName());
            if (in.getDescription() != null) cur.setDescription(in.getDescription());
            if (in.getStatus() != null) cur.setStatus(in.getStatus());
            cur.setUpdatedAt(LocalDateTime.now());
            roleMapper.updateById(cur);
        }
        return in;
    }

    public void delete(Long id) {
        roleMapper.deleteById(id);
        roleMapper.deleteRoleMenus(id);
    }

    @Transactional
    public void assignMenus(Long roleId, List<Long> menuIds) {
        getById(roleId);
        roleMapper.deleteRoleMenus(roleId);
        if (menuIds != null) {
            for (Long menuId : menuIds) {
                RoleMenuEntity rm = new RoleMenuEntity();
                rm.setRoleId(roleId);
                rm.setMenuId(menuId);
                roleMenuMapper.insert(rm);
            }
        }
    }

    public List<Long> getRoleMenuIds(Long roleId) {
        return roleMapper.selectMenuIdsByRoleId(roleId);
    }
}
