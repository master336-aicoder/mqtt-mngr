package io.mngr.web;

import io.mngr.core.model.Page;
import io.mngr.store.h2.entity.RoleEntity;
import io.mngr.sys.RoleService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/roles")
@RequiredArgsConstructor
public class RoleController {

    private final RoleService roleService;
    private final AuditHelper audit;

    @GetMapping
    public ApiResponse<Page<RoleEntity>> list(@RequestParam(required = false) String keyword,
                                               @RequestParam(defaultValue = "0") int page,
                                               @RequestParam(defaultValue = "20") int size) {
        List<RoleEntity> items = roleService.list(keyword, page, size);
        long total = roleService.count(keyword);
        return ApiResponse.ok(new Page<>(items, total));
    }

    @GetMapping("/{id}")
    public ApiResponse<RoleEntity> get(@PathVariable Long id) {
        return ApiResponse.ok(roleService.getById(id));
    }

    @PostMapping
    public ApiResponse<RoleEntity> save(@RequestBody RoleEntity in) {
        RoleEntity saved = roleService.save(in);
        audit.record(in.getId() == null ? "CREATE" : "UPDATE", "ROLE",
                String.valueOf(saved.getId()), null, saved);
        return ApiResponse.ok(saved);
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        roleService.delete(id);
        audit.record("DELETE", "ROLE", String.valueOf(id), null, null);
        return ApiResponse.ok(null);
    }

    @PostMapping("/{id}/menus")
    public ApiResponse<Void> assignMenus(@PathVariable Long id, @RequestBody List<Long> menuIds) {
        roleService.assignMenus(id, menuIds);
        audit.record("ASSIGN_MENUS", "ROLE", String.valueOf(id), null, menuIds);
        return ApiResponse.ok(null);
    }

    @GetMapping("/{id}/menus")
    public ApiResponse<List<Long>> getRoleMenuIds(@PathVariable Long id) {
        return ApiResponse.ok(roleService.getRoleMenuIds(id));
    }
}
