package io.mngr.web;

import io.mngr.config.MngrProperties;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** X-Mngr-Token 鉴权（配置非空才启用）。 */
@RequiredArgsConstructor
public class AuthFilter implements Filter {

    private final MngrProperties props;

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest r = (HttpServletRequest) req;
        String token = props.getAuth().getToken();
        String path = r.getRequestURI();
        boolean open = token == null || token.isBlank()
                || !path.startsWith("/api/")
                || path.startsWith("/api/v1/public/")
                || path.startsWith("/actuator/health");
        if (open) {
            chain.doFilter(req, res);
            return;
        }
        String given = r.getHeader("X-Mngr-Token");
        if (given != null && constantTimeEquals(given, token)) {
            chain.doFilter(req, res);
            return;
        }
        HttpServletResponse h = (HttpServletResponse) res;
        h.setStatus(401);
        h.setContentType("application/json;charset=UTF-8");
        h.getWriter().write("{\"code\":401,\"message\":\"invalid or missing X-Mngr-Token\"}");
    }

    static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
