package io.github.alexoooo.sample.async.generic;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;


public final class DynamicSemaphore
{
    //-----------------------------------------------------------------------------------------------------------------
    private static class InternalSemaphore extends Semaphore {
        InternalSemaphore(int permits) {
            super(permits, true);
        }

        @Override
        protected void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final int softLimit;
    private final InternalSemaphore semaphore;
    private final ReentrantLock expansionLock = new ReentrantLock(true);
    private final AtomicInteger temporaryLimit = new AtomicInteger(-1);


    //-----------------------------------------------------------------------------------------------------------------
    public DynamicSemaphore(int softLimit) {
        this.softLimit = softLimit;
        this.semaphore = new InternalSemaphore(softLimit);
    }


    //-----------------------------------------------------------------------------------------------------------------
    public int getSoftLimit() {
        return softLimit;
    }

    public int getCurrentLimit() {
        int currentTemporaryLimit = temporaryLimit.get();
        return currentTemporaryLimit == -1
                ? softLimit
                : currentTemporaryLimit;
    }

    public int getAvailable() {
        return semaphore.availablePermits();
    }

    public int getUsed() {
        return getCurrentLimit() - getAvailable();
    }


    //-----------------------------------------------------------------------------------------------------------------
    public void acquire(int cost) throws InterruptedException {
        if (cost <= softLimit) {
            semaphore.acquire(cost);
        }
        else {
            expansionLock.lock();
            try {
                temporaryLimit.set(cost);
                int expansion = cost - softLimit;
                semaphore.release(expansion);
                semaphore.acquire(cost);
            }
            finally {
                expansionLock.unlock();
            }
        }
    }


    public void release(int cost) {
        if (cost <= softLimit) {
            semaphore.release(cost);
        }
        else {
            synchronized (semaphore) {
                temporaryLimit.set(-1);
                int expansion = cost - softLimit;
                semaphore.reducePermits(expansion);
                semaphore.release(cost);
            }
        }
    }
}
