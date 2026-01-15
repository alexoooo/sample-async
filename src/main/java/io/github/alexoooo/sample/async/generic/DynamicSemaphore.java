package io.github.alexoooo.sample.async.generic;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;


public final class DynamicSemaphore
{
    //-----------------------------------------------------------------------------------------------------------------
    private final Sync sync;


    //-----------------------------------------------------------------------------------------------------------------
    public DynamicSemaphore(int softMax) {
        if (softMax <= 0) throw new IllegalArgumentException("softMax must be > 0");
        this.sync = new Sync(softMax);
    }


    //-----------------------------------------------------------------------------------------------------------------
    public void acquire(int cost) throws InterruptedException {
        if (cost <= 0) throw new IllegalArgumentException("cost must be > 0");
        sync.acquireSharedInterruptibly(cost);
    }

    public boolean tryAcquire(int cost, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (cost <= 0) throw new IllegalArgumentException("cost must be > 0");
        return sync.tryAcquireSharedNanos(cost, unit.toNanos(timeout));
    }

    public void release(int cost) {
        if (cost <= 0) throw new IllegalArgumentException("cost must be > 0");
        sync.releaseShared(cost);
    }

    public int getUsed() {
        return sync.getUsed();
    }

    public int getCurrentLimit() {
        return sync.currentLimit;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private static final class Sync extends AbstractQueuedSynchronizer {
        final int softMax;

        volatile int currentLimit;
        volatile boolean oversizedPending;

        Sync(int softMax) {
            this.softMax = softMax;
            this.currentLimit = softMax;
        }

        int getUsed() {
            return getState();
        }

        @Override
        protected int tryAcquireShared(int cost) {
            for (;;) {
                int used = getState();

                // If an oversized task is pending, block new normal tasks
                if (oversizedPending && cost <= softMax) {
                    return -1;
                }

                // Oversized request
                if (cost > softMax) {
                    // Must run alone
                    if (used != 0) return -1;

                    // Claim oversized mode
                    if (!oversizedPending) {
                        oversizedPending = true;
                        currentLimit = cost;
                    }

                    if (compareAndSetState(0, cost)) {
                        return 1;
                    }
                    return -1;
                }

                // Normal request
                int limit = currentLimit;
                if (used + cost > limit) {
                    return -1;
                }

                if (compareAndSetState(used, used + cost)) {
                    return 1;
                }
            }
        }

        @Override
        protected boolean tryReleaseShared(int cost) {
            for (;;) {
                int used = getState();
                int next = used - cost;
                if (next < 0) {
                    throw new IllegalStateException("release exceeds acquired permits");
                }

                if (compareAndSetState(used, next)) {
                    // If an oversized task just finished, restore soft limit
                    if (next == 0 && oversizedPending) {
                        oversizedPending = false;
                        currentLimit = softMax;
                    }
                    return true;
                }
            }
        }
    }
}
