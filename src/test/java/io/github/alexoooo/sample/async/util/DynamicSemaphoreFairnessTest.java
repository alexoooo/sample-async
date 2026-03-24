package io.github.alexoooo.sample.async.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;


public class DynamicSemaphoreFairnessTest
{
    //-----------------------------------------------------------------------------------------------------------------
    private DynamicSemaphore create(int softLimit) {
        return new DynamicSemaphore(softLimit, true);
    }
//    private DynamicSemaphoreV2 create(int softLimit) {
//        return new DynamicSemaphoreV2(softLimit);
//    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    void fairModeDoesNotLetLaterSmallAcquireBypassEarlierBlockedLarge() throws Exception
    {
        var semaphore = create(2);

        // Keep one permit occupied so that:
        // - A (needs 2) must wait
        // - B (needs 1) would be able to run immediately in the broken implementation
        semaphore.acquire(1);

        List<String> order = new CopyOnWriteArrayList<>();

        CountDownLatch aAcquired = new CountDownLatch(1);
        CountDownLatch bAcquired = new CountDownLatch(1);
        CountDownLatch aMayRelease = new CountDownLatch(1);
        CountDownLatch bMayRelease = new CountDownLatch(1);

        AtomicReference<Throwable> aError = new AtomicReference<>();
        AtomicReference<Throwable> bError = new AtomicReference<>();

        Thread a = new Thread(() -> {
            try {
                semaphore.acquire(2);
                order.add("A");
                aAcquired.countDown();
                aMayRelease.await();
                semaphore.release(2);
            }
            catch (Throwable t) {
                aError.set(t);
            }
        }, "A");

        Thread b = new Thread(() -> {
            try {
                semaphore.acquire(1);
                order.add("B");
                bAcquired.countDown();
                bMayRelease.await();
                semaphore.release(1);
            }
            catch (Throwable t) {
                bError.set(t);
            }
        }, "B");

        a.start();
        awaitState(a, Thread.State.WAITING, Duration.ofSeconds(1), "A");

        b.start();

        boolean mainPermitReleased = false;
        try {
            // In the broken implementation, B typically acquires here even though A arrived first.
            assertFalse(bAcquired.await(150, TimeUnit.MILLISECONDS),
                    "B should not acquire before A when fair=true");

            semaphore.release(1);
            mainPermitReleased = true;

            assertTrue(aAcquired.await(1, TimeUnit.SECONDS),
                    "A did not acquire after the held permit was released");

            aMayRelease.countDown();

            assertTrue(bAcquired.await(1, TimeUnit.SECONDS),
                    "B did not acquire after A completed");

            bMayRelease.countDown();

            a.join(1000);
            b.join(1000);

            if (a.isAlive()) {
                fail("Thread A did not terminate");
            }
            if (b.isAlive()) {
                fail("Thread B did not terminate");
            }

            if (aError.get() != null) {
                throw new AssertionError("Thread A failed", aError.get());
            }
            if (bError.get() != null) {
                throw new AssertionError("Thread B failed", bError.get());
            }

            assertEquals(List.of("A", "B"), order);
        }
        finally {
            aMayRelease.countDown();
            bMayRelease.countDown();

            if (!mainPermitReleased) {
                semaphore.release(1);
            }

            a.join(1000);
            b.join(1000);
        }
    }

    @SuppressWarnings({"SameParameterValue", "BusyWait"})
    private static void awaitState(Thread thread, Thread.State expected, Duration timeout, String name)
            throws InterruptedException
    {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (thread.getState() == expected) {
                return;
            }
            Thread.sleep(1);
        }
        fail(name + " did not reach state " + expected + "; current state=" + thread.getState());
    }



    //-----------------------------------------------------------------------------------------------------------------
    @Test
    void testStrictFifoFairness_PreventsBarging() throws InterruptedException {
        var semaphore = create(5);

        // Main thread acquires all 5 permits so the semaphore is fully exhausted.
        semaphore.acquire(5);
        assertEquals(0, semaphore.getAvailable());

        AtomicBoolean t2Acquired = new AtomicBoolean(false);
        AtomicBoolean t3Acquired = new AtomicBoolean(false);

        // Thread 2 (The Older Thread): Wants 3 permits.
        Thread t2 = new Thread(() -> {
            try {
                semaphore.acquire(3);
                t2Acquired.set(true);
                semaphore.release(3);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "Thread-2-Needs-3");

        // Thread 3 (The Newer Thread): Wants 1 permit.
        Thread t3 = new Thread(() -> {
            try {
                semaphore.acquire(1);
                t3Acquired.set(true);
                semaphore.release(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "Thread-3-Needs-1");

        // 1. Start T2 and wait until it is definitively blocked in the semaphore.
        t2.start();
        awaitWaitingState(t2);

        // 2. Start T3 and wait until it is definitively blocked behind T2.
        t3.start();
        awaitWaitingState(t3);

        // 3. THE CRITICAL ACTION: Release exactly 1 permit.
        // - T2 is first in line, but needs 3, so it cannot proceed yet.
        // - T3 is second in line, needs 1 (which is now available).
        semaphore.release(1);

        // Give the threads a brief moment to react to the release.
        Thread.sleep(200);

        // 4. THE ASSERTION:
        // If the semaphore is fair, T3 must STILL be waiting because it is behind T2.
        // If the semaphore has the barging bug, T3 will have snatched the 1 permit and finished.
        assertFalse(t3Acquired.get());
        assertEquals(Thread.State.WAITING, t3.getState());

        // 5. Clean up: Release the remaining 4 permits so the threads can finish.
        semaphore.release(4);

        // Wait for threads to finish cleanly and assert they eventually succeeded.
        t2.join(1000);
        t3.join(1000);

        assertTrue(t2Acquired.get());
        assertTrue(t3Acquired.get());
    }


    @SuppressWarnings("BusyWait")
    private void awaitWaitingState(Thread thread) throws InterruptedException {
        while (thread.getState() != Thread.State.WAITING) {
            Thread.sleep(10);
            if (thread.getState() == Thread.State.TERMINATED) {
                fail("Thread terminated unexpectedly early without waiting: " + thread.getName());
            }
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    @Timeout(10)
    void fair_preventsBargingByLateSmallRequest() throws Exception {
        var semaphore = create(2);

        semaphore.acquire(2); // occupy all permits

        List<String> order = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch bEnteredAcquire = new CountDownLatch(1);
        CountDownLatch cEnteredAcquire = new CountDownLatch(1);
        CountDownLatch dEnteredAcquire = new CountDownLatch(1);

        CountDownLatch bAcquired = new CountDownLatch(1);
        CountDownLatch bMayRelease = new CountDownLatch(1);

        Thread B = new Thread(() -> {
            try {
                bEnteredAcquire.countDown();
                semaphore.acquire(2);
                order.add("B");
                bAcquired.countDown();
                bMayRelease.await();
                semaphore.release(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "B");

        Thread C = new Thread(() -> {
            try {
                cEnteredAcquire.countDown();
                semaphore.acquire(2);
                order.add("C");
                semaphore.release(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "C");

        Thread D = new Thread(() -> {
            try {
                dEnteredAcquire.countDown();
                semaphore.acquire(1);
                order.add("D");
                semaphore.release(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, "D");

        // ---- start B first ----
        B.start();
        assertTrue(bEnteredAcquire.await(1, TimeUnit.SECONDS));
        awaitBlocked(B);

        // ---- start C second ----
        C.start();
        assertTrue(cEnteredAcquire.await(1, TimeUnit.SECONDS));
        awaitBlocked(C);

        // ---- start D third ----
        D.start();
        assertTrue(dEnteredAcquire.await(1, TimeUnit.SECONDS));
        awaitBlocked(D);

        // Now queue order is guaranteed: B -> C -> D

        semaphore.release(2);

        assertTrue(bAcquired.await(1, TimeUnit.SECONDS));

        bMayRelease.countDown();

        B.join();
        C.join();
        D.join();

        // fair => B, C, D
        // unfair => B, D, C
        assertEquals(List.of("B", "C", "D"), order);
    }


    private static void awaitBlocked(Thread t) throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            var s = t.getState();
            if (s == Thread.State.WAITING
                    || s == Thread.State.TIMED_WAITING
                    || s == Thread.State.BLOCKED) {
                return;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("Thread not blocked: " + t.getName());
    }
}
