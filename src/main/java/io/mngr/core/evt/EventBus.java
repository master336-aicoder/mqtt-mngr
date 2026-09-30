package io.mngr.core.evt;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** 轻量事件总线（Guava-free）：Server 状态迁移、订阅变更等，WS 层订阅后广播。 */
@Slf4j
public class EventBus {

    public record MngrEvent(String type, String subject, Map<String, Object> data) {}

    private final List<Consumer<MngrEvent>> listeners = new CopyOnWriteArrayList<>();

    public void register(Consumer<MngrEvent> l) { listeners.add(l); }

    public void unregister(Consumer<MngrEvent> l) { listeners.remove(l); }

    public void fire(String type, String subject, Map<String, Object> data) {
        MngrEvent e = new MngrEvent(type, subject, data);
        for (Consumer<MngrEvent> l : listeners) {
            try {
                l.accept(e);
            } catch (Exception ex) {
                log.warn("event listener error", ex);
            }
        }
    }
}
