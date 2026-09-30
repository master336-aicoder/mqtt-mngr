package io.mngr.web;

import io.mngr.store.h2.entity.MenuEntity;
import io.mngr.sys.MenuService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/menus")
@RequiredArgsConstructor
public class MenuController {

    private final MenuService menuService;
    private final AuditHelper audit;

    @GetMapping
    public ApiResponse<List<MenuEntity>> list() {
        return ApiResponse.ok(menuService.listAll());
    }

    @GetMapping("/{id}")
    public ApiResponse<MenuEntity> get(@PathVariable Long id) {
        return ApiResponse.ok(menuService.getById(id));
    }

    @PostMapping
    public ApiResponse<MenuEntity> save(@RequestBody MenuEntity in) {
        MenuEntity saved = menuService.save(in);
        audit.record(in.getId() == null ? "CREATE" : "UPDATE", "MENU",
                String.valueOf(saved.getId()), null, saved);
        return ApiResponse.ok(saved);
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        menuService.delete(id);
        audit.record("DELETE", "MENU", String.valueOf(id), null, null);
        return ApiResponse.ok(null);
    }
}
