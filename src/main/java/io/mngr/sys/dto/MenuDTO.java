package io.mngr.sys.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class MenuDTO {
    private Long id;
    private Long parentId;
    private String menuName;
    private Integer menuType;
    private String path;
    private String icon;
    private String permCode;
    private Integer sortOrder;
    private Integer status;
    private List<MenuDTO> children;
}
