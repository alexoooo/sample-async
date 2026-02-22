package io.github.alexoooo.sample.async.generic;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A semaphore with a soft permit limit that allows a single "big" request to temporarily
 * expand the limit beyond {@code softLimit}.
 *
 * <p>A request is <em>small</em> if {@code permits <= softLimit} and <em>big</em> otherwise.
 * Small requests compete for up to {@code softLimit} permits in the usual way.
 * A big request waits for all in-flight permits to drain, then runs alone with the limit
 * temporarily expanded to {@code softLimit + bigRequestPermits}, then restores it on release.
 * Only one big request may be active (or draining) at a time.
 *
 * <p>Thread-safe. All mutable state is guarded by a single {@link ReentrantLock}.
 */
public final class DynamicSemaphore
{
    //-----------------------------------------------------------------------------------------------------------------
    private final int softLimit;
    private int temporaryLimit;

    private final ReentrantLock lock = new ReentrantLock();

    /** Signalled whenever {@code used} decreases, i.e. permits are released */
    private final Condition permitAvailable = lock.newCondition();

    /** Signalled when the big-request slot is freed */
    private final Condition bigSlotAvailable = lock.newCondition();

    /** Permits currently held by all callers (big and small) */
    private int used;


    //-----------------------------------------------------------------------------------------------------------------
    public DynamicSemaphore(int softLimit) {
        if (softLimit < 1) throw new IllegalArgumentException("softLimit must be >= 1");
        this.softLimit = softLimit;
    }


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * Acquires {@code permits} permits, blocking until they are available.
     * Big requests ({@code permits > softLimit}) wait for a drain and run alone.
     */
    public void acquire(int permits) throws InterruptedException {
        if (permits < 1) throw new IllegalArgumentException("permits must be >= 1");

        boolean isBig = permits > softLimit;

        lock.lockInterruptibly();
        try {
            if (isBig) {
                // Wait for any prior big request to fully complete, then claim the slot.
                while (temporaryLimit != 0) {
                    bigSlotAvailable.await();
                }
                temporaryLimit = permits; // blocks new small acquires from here on

                // Wait for all in-flight permits to drain so we run alone.
                while (used > 0) {
                    permitAvailable.await();
                }
                // Limit is now effectively softLimit + permits; grab them atomically.
            }
            else {
                // Block while a big request owns the semaphore, or there isn't room.
                while (temporaryLimit != 0 || used + permits > softLimit) {
                    permitAvailable.await();
                }
            }

            used += permits;
        }
        catch (InterruptedException e) {
            if (isBig) {
                // We held the big slot but never acquired permits — release the slot.
                temporaryLimit = 0;
                bigSlotAvailable.signal();
                permitAvailable.signalAll(); // unblock small waiters we were holding back
            }
            throw e;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Releases {@code permits} permits previously obtained via {@link #acquire}.
     * Must be called with the same value that was passed to acquire.
     */
    public void release(int permits) {
        if (permits < 1) throw new IllegalArgumentException("permits must be >= 1");

        lock.lock();
        try {
            if (used < permits)
                throw new IllegalStateException("Released more permits than acquired");

            used -= permits;

            if (permits == temporaryLimit) {
                // Big request is done: surrender the slot before waking waiters.
                temporaryLimit = 0;
                bigSlotAvailable.signal();
            }

            permitAvailable.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    //---- Accessors --------------------------------------------------------------------------------------------------
    /** The configured soft limit; constant after construction. */
    public int getSoftLimit() {
        return softLimit; // final field, no lock needed
    }

    /**
     * The current effective limit: {@code softLimit} normally, or
     * {@code softLimit + bigRequestPermits} while a big request is active.
     */
    public int getCurrentLimit() {
        lock.lock();
        try { return softLimit + temporaryLimit; }
        finally { lock.unlock(); }
    }

    /** Total permits currently held by all callers. */
    public int getUsed() {
        lock.lock();
        try { return used; }
        finally { lock.unlock(); }
    }

    /** Permits available right now without blocking. */
    public int getAvailable() {
        lock.lock();
        try { return softLimit + temporaryLimit - used; }
        finally { lock.unlock(); }
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            int limit = softLimit + temporaryLimit;
            return "DynamicSemaphore{used=" + used
                    + ", available=" + (limit - used)
                    + ", currentLimit=" + limit
                    + ", softLimit=" + softLimit
                    + ", bigRequestActive=" + (temporaryLimit != 0) + "}";
        }
        finally {
            lock.unlock();
        }
    }
}
