package io.mngr.core.server;

import io.mngr.core.enums.ServerState;
import io.mngr.core.model.ServerRuntime;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 每 Server 一个 ConnSlot 的状态机持有者（§7.1）。 */
@Slf4j
public class ConnectionRegistry {

    public static class ConnSlot {
        public volatile ServerRuntime runtime;
        public volatile Object handle;              // adapter 内部句柄
        public volatile java.util.concurrent.ScheduledFuture<?> retryTask;
    }

    private final Map<String, ConnSlot> slots = new ConcurrentHashMap<>();

    public ConnSlot slot(String serverId) {
        return slots.computeIfAbsent(serverId, id -> {
            ConnSlot s = new ConnSlot();
            s.runtime = ServerRuntime.builder().serverId(id).state(ServerState.INIT).build();
            return s;
        });
    }

    public synchronized void transition(String serverId, ServerState to, String err, Map<String, Object> info) {
        ConnSlot s = slot(serverId);
        ServerState from = s.runtime.getState();
        s.runtime = s.runtime.toBuilder()
                .state(to)
                .lastError(err)
                .lastErrorAt(err == null ? s.runtime.getLastErrorAt() : Instant.now())
                .connectedSince(to == ServerState.CONNECTED ? Instant.now()
                        : (to == ServerState.DEGRADED || to == ServerState.DOWN ? null : s.runtime.getConnectedSince()))
                .protocolInfo(info == null ? s.runtime.getProtocolInfo() : info)
                .retryCount(to == ServerState.CONNECTED ? 0
                        : (to == ServerState.DEGRADED ? s.runtime.getRetryCount() + 1 : s.runtime.getRetryCount()))
                .build();
        if (from != to) {
            log.info("server {} state {} -> {} {}", serverId, from, to, err == null ? "" : "(" + err + ")");
        }
    }

    public ServerRuntime runtime(String serverId) { return slot(serverId).runtime; }

    public Map<String, ServerRuntime> snapshot() {
        Map<String, ServerRuntime> m = new ConcurrentHashMap<>();
        slots.forEach((k, v) -> m.put(k, v.runtime));
        return m;
    }

    public boolean isUp(String serverId) {
        return slot(serverId).runtime.getState() == ServerState.CONNECTED;
    }

    public void remove(String serverId) { slots.remove(serverId); }
}
