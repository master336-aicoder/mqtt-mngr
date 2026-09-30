package io.mngr.web;

import io.mngr.config.MngrProperties;
import io.mngr.sys.SessionManager;
import io.mngr.sys.UserContext;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** 双模式鉴权：会话 Token（登录获取）+ 静态 Token（X-Mngr-Token 配置值）。 */
public class AuthFilter implements Filter {

    private final MngrProperties props;
    private final SessionManager sessionManager;

    public AuthFilter(MngrProperties props, SessionManager sessionManager) {
        this.props = props;
        this.sessionManager = sessionManager;
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest r = (HttpServletRequest) req;
        String path = r.getRequestURI();
        try {
            if (!path.startsWith("/api/") || path.startsWith("/actuator/health")) {
                chain.doFilter(req, res);
                return;
            }
            if (path.startsWith("/api/v1/public/")) {
                chain.doFilter(req, res);
                return;
            }

            String headerToken = r.getHeader("X-Mngr-Token");

            // 1. 尝试会话 Token
            SessionManager.SessionInfo session = sessionManager.validateSession(headerToken);
            if (session != null) {
                UserContext.set(new UserContext.UserInfo(session.userId(), session.username(), session.nickname()));
                chain.doFilter(req, res);
                return;
            }

            // 2. 回退到静态 Token（向后兼容）
            String staticToken = props.getAuth().getToken();
            if (staticToken != null && !staticToken.isBlank() && headerToken != null
                    && constantTimeEquals(headerToken, staticToken)) {
                UserContext.set(new UserContext.UserInfo(0L, "system", "系统"));
                chain.doFilter(req, res);
                return;
            }

            HttpServletResponse h = (HttpServletResponse) res;
            h.setStatus(401);
            h.setContentType("application/json;charset=UTF-8");
            h.getWriter().write("{\"code\":401,\"message\":\"未登录或登录已过期\"}");
        } finally {
            UserContext.clear();
        }
    }

    static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
