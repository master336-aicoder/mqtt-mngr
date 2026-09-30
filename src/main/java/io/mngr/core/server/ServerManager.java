package io.mngr.core.server;

import io.mngr.config.MngrProperties;
import io.mngr.core.enums.ConnMode;
import io.mngr.core.enums.ErrorCode;
import io.mngr.core.enums.Protocol;
import io.mngr.core.enums.ServerState;
import io.mngr.core.evt.EventBus;
import io.mngr.core.exception.MngrException;
import io.mngr.core.model.ServerConfig;
import io.mngr.core.model.ServerRuntime;
import io.mngr.core.spi.ConfigRepository;
import io.mngr.core.spi.ProtocolAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Server 生命周期编排：建连、状态机、退避重连、重连钩子（§7.1）。 */
@Slf4j
public class ServerManager implements AutoCloseable {

    private final ConfigRepository repo;
    private final Map<Protocol, ProtocolAdapter> adapters;
    private final ConnectionRegistry registry;
    private final EventBus bus;
    private final BackoffPolicy backoff = new BackoffPolicy();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "mngr-conn-mgr");
        t.setDaemon(true);
        return t;
    });
    private final List<Consumer<String>> reconnectHooks = new CopyOnWriteArrayList<>();
    private final MngrProperties props;

    public ServerManager(ConfigRepository repo, List<ProtocolAdapter> adapterList,
                         ConnectionRegistry registry, EventBus bus, MngrProperties props) {
        this.repo = repo;
        this.adapters = new java.util.HashMap<>();
        adapterList.forEach(a -> adapters.put(a.protocol(), a));
        this.registry = registry;
        this.bus = bus;
        this.props = props;
    }

    public ProtocolAdapter adapter(Protocol p) {
        ProtocolAdapter a = adapters.get(p);
        if (a == null) throw new MngrException(ErrorCode.VALIDATION, "无协议适配器: " + p);
        return a;
    }

    public ProtocolAdapter adapterFor(String serverId) {
        return adapter(getConfig(serverId).getProtocol());
    }

    public ServerConfig getConfig(String serverId) {
        return repo.findServer(serverId)
                .orElseThrow(() -> new MngrException(ErrorCode.NOT_FOUND, "server 不存在: " + serverId));
    }

    public void start() {
        for (ServerConfig c : repo.listServers(null, null, 0, 10000)) {
            registry.slot(c.getId());
            if (c.isEnabled() && c.getConnMode() == ConnMode.EAGER) {
                connect(c.getId());
            }
        }
    }

    public void addReconnectHook(Consumer<String> hook) { reconnectHooks.add(hook); }

    public ServerRuntime status(String serverId) { return registry.runtime(serverId); }

    public Map<String, ServerRuntime> snapshot() { return registry.snapshot(); }

    /** LAZY 语义：使用前确保连接；MANUAL 不自动连。 */
    public void ensureConnected(String serverId) {
        ServerState st = registry.runtime(serverId).getState();
        if (st == ServerState.CONNECTED) return;
        ServerConfig c = getConfig(serverId);
        if (!c.isEnabled()) throw new MngrException(ErrorCode.NOT_CONNECTED, "server 已停用");
        if (c.getConnMode() == ConnMode.MANUAL) {
            throw new MngrException(ErrorCode.NOT_CONNECTED, "MANUAL 模式需手动调用 connect");
        }
        connect(serverId);
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline
                && registry.runtime(serverId).getState() != ServerState.CONNECTED) {
            try { Thread.sleep(100); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (registry.runtime(serverId).getState() != ServerState.CONNECTED) {
            throw new MngrException(ErrorCode.NOT_CONNECTED,
                    "连接未就绪: " + registry.runtime(serverId).getLastError());
        }
    }

    /** 幂等触发建连（REST connect / 启动 / 重连共用）。 */
    public synchronized void connect(String serverId) {
        ServerConfig c = getConfig(serverId);
        ConnectionRegistry.ConnSlot slot = registry.slot(serverId);
        ServerState st = slot.runtime.getState();
        if (st == ServerState.CONNECTED || st == ServerState.CONNECTING) return;
        registry.transition(serverId, ServerState.CONNECTING, null, null);
        ProtocolAdapter a = adapter(c.getProtocol());
        a.connect(c, new ProtocolAdapter.ConnCallback() {
            @Override
            public void onConnected(String id, Map<String, Object> info) {
                registry.transition(id, ServerState.CONNECTED, null, info);
                bus.fire("server_state", id, Map.of("state", "CONNECTED"));
                for (Consumer<String> hook : reconnectHooks) {
                    try { hook.accept(id); } catch (Exception e) { log.warn("reconnect hook error", e); }
                }
            }

            @Override
            public void onDisconnected(String id, Throwable cause) {
                bus.fire("server_state", id, Map.of("state", "DISCONNECTED",
                        "error", cause == null ? "" : String.valueOf(cause.getMessage())));
                scheduleReconnect(id, cause);
            }
        });
    }

    private void scheduleReconnect(String serverId, Throwable cause) {
        ConnectionRegistry.ConnSlot slot = registry.slot(serverId);
        int rc = slot.runtime.getRetryCount() + 1;
        registry.transition(serverId, rc >= 10 ? ServerState.DOWN : ServerState.DEGRADED,
                cause == null ? "disconnected" : String.valueOf(cause.getMessage()), null);
        if (slot.retryTask != null && !slot.retryTask.isDone()) return;
        long delay = rc >= 10 ? 60_000 : backoff.nextDelayMs(rc);
        log.debug("schedule reconnect server={} in {}ms (attempt {})", serverId, delay, rc);
        slot.retryTask = scheduler.schedule(() -> {
            slot.retryTask = null;
            try {
                ServerConfig cfg = repo.findServer(serverId).orElse(null);
                if (cfg == null || !cfg.isEnabled()) return;
                registry.transition(serverId, ServerState.CONNECTING, null, null);
                adapter(cfg.getProtocol()).connect(cfg, new ProtocolAdapter.ConnCallback() {
                    @Override
                    public void onConnected(String id, Map<String, Object> info) {
                        registry.transition(id, ServerState.CONNECTED, null, info);
                        bus.fire("server_state", id, Map.of("state", "CONNECTED"));
                        for (Consumer<String> hook : reconnectHooks) {
                            try { hook.accept(id); } catch (Exception e) { log.warn("hook", e); }
                        }
                    }

                    @Override
                    public void onDisconnected(String id, Throwable c2) {
                        scheduleReconnect(id, c2);
                    }
                });
            } catch (Exception e) {
                registry.transition(serverId, ServerState.DOWN, String.valueOf(e.getMessage()), null);
                scheduleReconnect(serverId, e);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    /** 手动断开（不再自动重连）。 */
    public void disconnect(String serverId) {
        ConnectionRegistry.ConnSlot slot = registry.slot(serverId);
        if (slot.retryTask != null) slot.retryTask.cancel(true);
        ServerConfig c = repo.findServer(serverId).orElse(null);
        if (c == null) return;
        try {
            adapter(c.getProtocol()).disconnect(serverId, java.time.Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("disconnect error server={}: {}", serverId, e.toString());
        }
        registry.transition(serverId, ServerState.INIT, null, null);
        bus.fire("server_state", serverId, Map.of("state", "INIT"));
    }

    /** 配置更新后：已连接则重建（凭据/地址变化）。 */
    public void applyChange(ServerConfig newCfg) {
        ServerState st = registry.runtime(newCfg.getId()).getState();
        if (st == ServerState.CONNECTED || st == ServerState.CONNECTING || st == ServerState.DEGRADED) {
            disconnect(newCfg.getId());
            if (newCfg.isEnabled()) connect(newCfg.getId());
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
