package io.mngr.web;

import io.mngr.core.model.Page;
import io.mngr.store.h2.entity.UserEntity;
import io.mngr.sys.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;
    private final AuditHelper audit;

    @GetMapping
    public ApiResponse<Page<UserEntity>> list(@RequestParam(required = false) String keyword,
                                               @RequestParam(defaultValue = "0") int page,
                                               @RequestParam(defaultValue = "20") int size) {
        List<UserEntity> items = userService.list(keyword, page, size);
        long total = userService.count(keyword);
        items.forEach(u -> u.setPassword(null));
        return ApiResponse.ok(new Page<>(items, total));
    }

    @GetMapping("/{id}")
    public ApiResponse<UserEntity> get(@PathVariable Long id) {
        UserEntity u = userService.getById(id);
        u.setPassword(null);
        return ApiResponse.ok(u);
    }

    @PostMapping
    public ApiResponse<UserEntity> save(@RequestBody UserEntity in) {
        UserEntity saved = userService.save(in);
        audit.record(in.getId() == null ? "CREATE" : "UPDATE", "USER",
                String.valueOf(saved.getId()), null, saved);
        return ApiResponse.ok(saved);
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        userService.delete(id);
        audit.record("DELETE", "USER", String.valueOf(id), null, null);
        return ApiResponse.ok(null);
    }

    @PostMapping("/{id}/roles")
    public ApiResponse<Void> assignRoles(@PathVariable Long id, @RequestBody List<Long> roleIds) {
        userService.assignRoles(id, roleIds);
        audit.record("ASSIGN_ROLES", "USER", String.valueOf(id), null, roleIds);
        return ApiResponse.ok(null);
    }

    @GetMapping("/{id}/roles")
    public ApiResponse<List<Map<String, Object>>> getUserRoles(@PathVariable Long id) {
        return ApiResponse.ok(userService.getUserRoles(id));
    }

    @PostMapping("/{id}/reset-password")
    public ApiResponse<Void> resetPassword(@PathVariable Long id, @RequestBody Map<String, String> body) {
        userService.resetPassword(id, body.get("newPassword"));
        audit.record("RESET_PASSWORD", "USER", String.valueOf(id), null, null);
        return ApiResponse.ok(null);
    }
}
