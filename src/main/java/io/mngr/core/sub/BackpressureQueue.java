package io.mngr.core.sub;

import io.mngr.core.enums.ServerState;
import io.mngr.core.model.SubscriptionDef;
import lombok.Data;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/** 每订阅背压队列（有界，满则丢最旧并计数——调试守护进程形态）。 */
public class BackpressureQueue {

    @Data
    public static class Enqueued {
        private final io.mngr.core.model.RawIncoming raw;
        private final long enqueuedAt;
    }

    private final BlockingDeque<Enqueued> deque;
    private final int capacity;
    private volatile long dropped;
    private volatile boolean paused;

    public BackpressureQueue(int capacity) {
        this.capacity = capacity;
        this.deque = new LinkedBlockingDeque<>(capacity);
    }

    public boolean offer(io.mngr.core.model.RawIncoming raw) {
        if (paused) return true; // 暂停期间入队失败丢弃由 worker 恢复后处理——直接放行
        boolean ok = deque.offerLast(new Enqueued(raw, System.currentTimeMillis()));
        if (!ok) {
            deque.pollFirst();
            dropped++;
            deque.offerLast(new Enqueued(raw, System.currentTimeMillis()));
        }
        return ok;
    }

    public Enqueued take() throws InterruptedException {
        return deque.takeFirst();
    }

    public Enqueued poll(java.time.Duration d) throws InterruptedException {
        return deque.pollFirst(d.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public int size() { return deque.size(); }
    public long droppedCount() { return dropped; }
    public void setPaused(boolean p) { this.paused = p; }
    public boolean isPaused() { return paused; }

    public void clear() { deque.clear(); }
}
