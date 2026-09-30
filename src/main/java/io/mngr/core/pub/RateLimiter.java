package io.mngr.core.pub;

/** 令牌桶（Guava-free）：msg/bytes 双桶，0=不限。 */
public class RateLimiter {

    private final double msgPerSec;
    private final long bytesPerSec;
    private double msgTokens;
    private long byteTokens;
    private long lastNs;

    public RateLimiter(double msgPerSec, long bytesPerSec) {
        this.msgPerSec = msgPerSec;
        this.bytesPerSec = bytesPerSec;
        this.msgTokens = msgPerSec;
        this.byteTokens = bytesPerSec;
        this.lastNs = System.nanoTime();
    }

    public synchronized boolean tryAcquire(int bytes) {
        if (msgPerSec <= 0 && bytesPerSec <= 0) return true;
        long now = System.nanoTime();
        double elapsed = (now - lastNs) / 1e9;
        lastNs = now;
        if (msgPerSec > 0) {
            msgTokens = Math.min(msgPerSec, msgTokens + elapsed * msgPerSec);
            if (msgTokens < 1) return false;
        }
        if (bytesPerSec > 0) {
            byteTokens = (long) Math.min(bytesPerSec, byteTokens + elapsed * bytesPerSec);
            if (byteTokens < bytes) return false;
        }
        if (msgPerSec > 0) msgTokens -= 1;
        if (bytesPerSec > 0) byteTokens -= bytes;
        return true;
    }
}
