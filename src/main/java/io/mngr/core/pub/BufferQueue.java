package io.mngr.core.pub;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;

/** 断连/失败缓冲队列：容量+溢出策略（DROP_OLDEST|DROP_NEWEST|BLOCK）。 */
public class BufferQueue {

    public record Buffered(Object request, byte[] payload, long ts, int attempts) {}

    private final ArrayDeque<Buffered> deque;
    private final int capacity;
    private final String overflow;
    private final AtomicLong dropped = new AtomicLong();

    public BufferQueue(int capacity, String overflow) {
        this.capacity = capacity;
        this.overflow = overflow;
        this.deque = new ArrayDeque<>();
    }

    public synchronized boolean offer(Buffered b) {
        if (deque.size() >= capacity) {
            switch (overflow) {
                case "DROP_NEWEST" -> {
                    dropped.incrementAndGet();
                    return false;
                }
                case "BLOCK" -> {
                    long deadline = System.currentTimeMillis() + 2000;
                    while (deque.size() >= capacity && System.currentTimeMillis() < deadline) {
                        try { wait(200); } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                    if (deque.size() >= capacity) {
                        dropped.incrementAndGet();
                        return false;
                    }
                }
                default -> deque.pollFirst(); // DROP_OLDEST
            }
        }
        deque.addLast(b);
        return true;
    }

    public synchronized Buffered poll() { return deque.pollFirst(); }

    public synchronized int size() { return deque.size(); }

    public long droppedCount() { return dropped.get(); }
}
