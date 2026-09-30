package io.mngr.web;

import io.mngr.sys.AuthService;
import io.mngr.sys.SessionManager;
import io.mngr.sys.UserContext;
import io.mngr.sys.dto.LoginRequest;
import io.mngr.sys.dto.LoginResponse;
import io.mngr.sys.dto.UserInfoDTO;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;
    private final SessionManager sessionManager;

    @PostMapping("/api/v1/public/login")
    public ApiResponse<LoginResponse> login(@RequestBody LoginRequest req) {
        LoginResponse resp = authService.login(req);
        return ApiResponse.ok(resp);
    }

    @PostMapping("/api/v1/public/logout")
    public ApiResponse<Void> logout(HttpServletRequest request) {
        String token = request.getHeader("X-Mngr-Token");
        authService.logout(token);
        return ApiResponse.ok(null);
    }

    @GetMapping("/api/v1/user/info")
    public ApiResponse<UserInfoDTO> userInfo() {
        UserContext.UserInfo u = UserContext.get();
        if (u == null) return ApiResponse.ok(null);
        return ApiResponse.ok(authService.getCurrentUser(u.userId()));
    }
}
