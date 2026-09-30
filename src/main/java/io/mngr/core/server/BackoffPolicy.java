package io.mngr.core.server;

import java.util.concurrent.ThreadLocalRandom;

/** 指数退避：1s ×2 封顶 30s ±20% 抖动。 */
public class BackoffPolicy {

    private final long baseMs = 1000;
    private final long capMs = 30_000;

    public long nextDelayMs(int retryCount) {
        long exp = Math.min(capMs, baseMs * (1L << Math.min(retryCount, 15)));
        double jitter = 1 + (ThreadLocalRandom.current().nextDouble() * 0.4 - 0.2);
        return Math.max(200, (long) (exp * jitter));
    }
}
