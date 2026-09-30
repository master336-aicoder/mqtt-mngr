package io.mngr.sys.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class UserInfoDTO {
    private Long id;
    private String username;
    private String nickname;
    private String email;
    private String phone;
    private String avatar;
    private List<String> roles;
    private List<String> permissions;
    private List<MenuDTO> menus;
}
