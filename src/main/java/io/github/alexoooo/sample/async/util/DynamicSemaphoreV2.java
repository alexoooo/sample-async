package io.github.alexoooo.sample.async.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Alternative implementation based on lock around standard library Semaphore (WIP, still flaky tests)
 */
public final class DynamicSemaphoreV2 {
    // -----------------------------------------------------------------------------------------------------------------
    private static class InternalSemaphore extends Semaphore {
        InternalSemaphore(int permits) {
            super(permits, true);
        }

        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    private final int softLimit;
    private final InternalSemaphore semaphore;

    private final ReentrantLock expansionLock = new ReentrantLock(true);
    private final Condition noBigActive = expansionLock.newCondition();
    private final Condition permitsFreed = expansionLock.newCondition();

    private int temporaryLimit = -1;
    private int used;


    // -----------------------------------------------------------------------------------------------------------------
    public DynamicSemaphoreV2(int softLimit) {

        if (softLimit <= 0) {
            throw new IllegalArgumentException();
        }

        this.softLimit = softLimit;
        this.semaphore = new InternalSemaphore(softLimit);
    }


    // -----------------------------------------------------------------------------------------------------------------
    public int getSoftLimit() {
        return softLimit;
    }

    public int getCurrentLimit() {
        expansionLock.lock();
        try {
            return temporaryLimit == -1 ? softLimit : temporaryLimit;
        }
        finally {
            expansionLock.unlock();
        }
    }

    public int getAvailable() {
        return semaphore.availablePermits();
    }

    public int getUsed() {
        expansionLock.lock();
        try {
            return used;
        }
        finally {
            expansionLock.unlock();
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    public void acquire(int cost) throws InterruptedException {
        if (cost < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }

        if (cost <= softLimit) {
            expansionLock.lockInterruptibly();
            try {
                while (temporaryLimit != -1) {
                    noBigActive.await();
                }
            }
            finally {
                expansionLock.unlock();
            }

            semaphore.acquire(cost);

            expansionLock.lock();
            try {
                used += cost;
            }
            finally {
                expansionLock.unlock();
            }
            return;
        }

        expansionLock.lockInterruptibly();
        int expansion = cost - softLimit;
        boolean expanded = false;
        try {
            while (temporaryLimit != -1) {
                noBigActive.await();
            }

            temporaryLimit = cost;

            semaphore.release(expansion);
            expanded = true;

            while (used != 0) {
                permitsFreed.await();
            }

            semaphore.acquire(cost);
            used += cost;
        }
        catch (InterruptedException e) {
            if (expanded) {
                semaphore.reducePermits(expansion);
                temporaryLimit = -1;
                noBigActive.signalAll();
            }
            throw e;
        }
        finally {
            expansionLock.unlock();
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    public void release(int cost) {
        if (cost < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
        expansionLock.lock();

        try {

            if (used < cost) {
                throw new IllegalStateException(
                        "Released more permits than acquired: cost=" + cost + " used=" + used);
            }

            used -= cost;

            permitsFreed.signalAll();

            if (cost <= softLimit) {
                semaphore.release(cost);
                return;
            }

            int expansion = cost - softLimit;

            temporaryLimit = -1;

            semaphore.reducePermits(expansion);
            semaphore.release(cost);

            noBigActive.signalAll();
        }
        finally {
            expansionLock.unlock();
        }
    }


    // -----------------------------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        expansionLock.lock();

        try {
            int limit = temporaryLimit == -1
                    ? softLimit
                    : temporaryLimit;

            return "DynamicSemaphore{used=" + used
                    + ", available=" + semaphore.availablePermits()
                    + ", currentLimit=" + limit
                    + ", softLimit=" + softLimit + "}";

        }
        finally {
            expansionLock.unlock();
        }
    }
}