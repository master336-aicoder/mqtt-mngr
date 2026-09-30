package io.mngr.sys;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.exception.MngrException;
import io.mngr.sys.dto.*;
import io.mngr.store.h2.entity.MenuEntity;
import io.mngr.store.h2.entity.UserEntity;
import io.mngr.store.h2.mapper.MenuMapper;
import io.mngr.store.h2.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserMapper userMapper;
    private final MenuMapper menuMapper;
    private final SessionManager sessionManager;
    private static final BCryptPasswordEncoder ENCODER = new BCryptPasswordEncoder();

    public LoginResponse login(LoginRequest req) {
        if (req.getUsername() == null || req.getUsername().isBlank()) {
            throw new MngrException(ErrorCode.VALIDATION, "用户名不能为空");
        }
        if (req.getPassword() == null || req.getPassword().isBlank()) {
            throw new MngrException(ErrorCode.VALIDATION, "密码不能为空");
        }
        UserEntity user = userMapper.selectOne(
                new QueryWrapper<UserEntity>().eq("username", req.getUsername()));
        if (user == null) {
            throw new MngrException(ErrorCode.UNAUTHORIZED, "用户名或密码错误");
        }
        if (user.getStatus() != 1) {
            throw new MngrException(ErrorCode.UNAUTHORIZED, "账号已被禁用");
        }
        if (!ENCODER.matches(req.getPassword(), user.getPassword())) {
            throw new MngrException(ErrorCode.UNAUTHORIZED, "用户名或密码错误");
        }
        String token = sessionManager.createSession(user.getId(), user.getUsername(), user.getNickname());
        UserInfoDTO info = buildUserInfo(user);
        return LoginResponse.builder().token(token).userInfo(info).build();
    }

    public UserInfoDTO getCurrentUser(Long userId) {
        UserEntity user = userMapper.selectById(userId);
        if (user == null) throw new MngrException(ErrorCode.NOT_FOUND, "用户不存在");
        return buildUserInfo(user);
    }

    public void logout(String token) {
        sessionManager.removeSession(token);
    }

    private UserInfoDTO buildUserInfo(UserEntity user) {
        List<Map<String, Object>> roles = userMapper.selectRolesByUserId(user.getId());
        List<String> roleCodes = roles.stream().map(r -> {
            Object code = r.get("role_code");
            if (code == null) code = r.get("ROLE_CODE");
            return (String) code;
        }).toList();
        List<MenuEntity> menus = menuMapper.selectMenusByUserId(user.getId());
        List<String> perms = menus.stream()
                .filter(m -> m.getPermCode() != null && !m.getPermCode().isBlank())
                .map(MenuEntity::getPermCode).distinct().toList();
        List<MenuDTO> menuTree = buildTree(menus);
        return UserInfoDTO.builder()
                .id(user.getId())
                .username(user.getUsername())
                .nickname(user.getNickname())
                .email(user.getEmail())
                .phone(user.getPhone())
                .avatar(user.getAvatar())
                .roles(roleCodes)
                .permissions(perms)
                .menus(menuTree)
                .build();
    }

    static List<MenuDTO> buildTree(List<MenuEntity> menus) {
        Map<Long, MenuDTO> map = new LinkedHashMap<>();
        for (MenuEntity m : menus) {
            map.put(m.getId(), MenuDTO.builder()
                    .id(m.getId()).parentId(m.getParentId()).menuName(m.getMenuName())
                    .menuType(m.getMenuType()).path(m.getPath()).icon(m.getIcon())
                    .permCode(m.getPermCode()).sortOrder(m.getSortOrder()).status(m.getStatus())
                    .children(new ArrayList<>()).build());
        }
        List<MenuDTO> roots = new ArrayList<>();
        for (MenuDTO dto : map.values()) {
            if (dto.getParentId() == null || dto.getParentId() == 0) {
                roots.add(dto);
            } else {
                MenuDTO parent = map.get(dto.getParentId());
                if (parent != null) parent.getChildren().add(dto);
                else roots.add(dto);
            }
        }
        return roots;
    }
}
