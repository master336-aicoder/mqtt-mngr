package io.mngr.sys;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.store.h2.entity.UserEntity;
import io.mngr.store.h2.entity.UserRoleEntity;
import io.mngr.store.h2.mapper.UserMapper;
import io.mngr.store.h2.mapper.UserRoleMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserMapper userMapper;
    private final UserRoleMapper userRoleMapper;
    private static final BCryptPasswordEncoder ENCODER = new BCryptPasswordEncoder();

    public List<UserEntity> list(String keyword, int page, int size) {
        QueryWrapper<UserEntity> w = new QueryWrapper<>();
        if (keyword != null && !keyword.isBlank()) {
            w.like("username", keyword).or().like("nickname", keyword);
        }
        w.orderByAsc("id").last("LIMIT " + size + " OFFSET " + (long) page * size);
        return userMapper.selectList(w);
    }

    public long count(String keyword) {
        QueryWrapper<UserEntity> w = new QueryWrapper<>();
        if (keyword != null && !keyword.isBlank()) {
            w.like("username", keyword).or().like("nickname", keyword);
        }
        return userMapper.selectCount(w);
    }

    public UserEntity getById(Long id) {
        UserEntity u = userMapper.selectById(id);
        if (u == null) throw new MngrException(ErrorCode.NOT_FOUND, "用户不存在: " + id);
        return u;
    }

    @Transactional
    public UserEntity save(UserEntity in) {
        boolean isNew = in.getId() == null;
        if (isNew) {
            if (in.getUsername() == null || in.getUsername().isBlank()) {
                throw new MngrException(ErrorCode.VALIDATION, "用户名不能为空");
            }
            if (in.getPassword() == null || in.getPassword().isBlank()) {
                throw new MngrException(ErrorCode.VALIDATION, "密码不能为空");
            }
            UserEntity existing = userMapper.selectOne(
                    new QueryWrapper<UserEntity>().eq("username", in.getUsername()));
            if (existing != null) {
                throw new MngrException(ErrorCode.DUP_NAME, "用户名已存在: " + in.getUsername());
            }
            in.setPassword(ENCODER.encode(in.getPassword()));
            if (in.getStatus() == null) in.setStatus(1);
            in.setCreatedAt(LocalDateTime.now());
            in.setUpdatedAt(LocalDateTime.now());
            userMapper.insert(in);
        } else {
            UserEntity cur = getById(in.getId());
            if (in.getPassword() != null && !in.getPassword().isBlank()) {
                cur.setPassword(ENCODER.encode(in.getPassword()));
            }
            if (in.getNickname() != null) cur.setNickname(in.getNickname());
            if (in.getEmail() != null) cur.setEmail(in.getEmail());
            if (in.getPhone() != null) cur.setPhone(in.getPhone());
            if (in.getAvatar() != null) cur.setAvatar(in.getAvatar());
            if (in.getStatus() != null) cur.setStatus(in.getStatus());
            cur.setUpdatedAt(LocalDateTime.now());
            userMapper.updateById(cur);
        }
        in.setPassword(null);
        return in;
    }

    public void delete(Long id) {
        userMapper.deleteById(id);
        userRoleMapper.deleteByUserId(id);
    }

    @Transactional
    public void assignRoles(Long userId, List<Long> roleIds) {
        getById(userId);
        userRoleMapper.deleteByUserId(userId);
        if (roleIds != null) {
            for (Long roleId : roleIds) {
                UserRoleEntity ur = new UserRoleEntity();
                ur.setUserId(userId);
                ur.setRoleId(roleId);
                userRoleMapper.insert(ur);
            }
        }
    }

    public List<Map<String, Object>> getUserRoles(Long userId) {
        return userMapper.selectRolesByUserId(userId);
    }

    public void resetPassword(Long userId, String newPassword) {
        UserEntity u = getById(userId);
        u.setPassword(ENCODER.encode(newPassword));
        u.setUpdatedAt(LocalDateTime.now());
        userMapper.updateById(u);
    }
}
