package io.mngr.sys;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** 会话管理（内存态，重启后需重新登录）。 */
@Component
public class SessionManager {

    private static final long SESSION_TTL_SECONDS = 86400L;

    private record Session(String username, Long userId, String nickname, Instant expiresAt) {}

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    public String createSession(Long userId, String username, String nickname) {
        String token = UUID.randomUUID().toString().replace("-", "");
        sessions.put(token, new Session(username, userId, nickname,
                Instant.now().plusSeconds(SESSION_TTL_SECONDS)));
        return token;
    }

    public SessionInfo validateSession(String token) {
        if (token == null || token.isBlank()) return null;
        Session s = sessions.get(token);
        if (s == null) return null;
        if (Instant.now().isAfter(s.expiresAt)) {
            sessions.remove(token);
            return null;
        }
        return new SessionInfo(s.userId, s.username, s.nickname);
    }

    public void removeSession(String token) {
        if (token != null) sessions.remove(token);
    }

    private void cleanExpired() {
        Instant now = Instant.now();
        sessions.entrySet().removeIf(e -> now.isAfter(e.getValue().expiresAt()));
    }

    public record SessionInfo(Long userId, String username, String nickname) {}
}
