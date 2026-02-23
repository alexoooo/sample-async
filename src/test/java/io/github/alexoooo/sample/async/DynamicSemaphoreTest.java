package io.github.alexoooo.sample.async;

import io.github.alexoooo.sample.async.generic.DynamicSemaphore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


public class DynamicSemaphoreTest {
    //-----------------------------------------------------------------------------------------------------------------
    private final static int softLimit = 10;

    DynamicSemaphore semaphore;

    @BeforeEach
    void setUp() {
        semaphore = new DynamicSemaphore(10);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    void basicAcquireAndRelease() throws InterruptedException {
        assertEquals(softLimit, semaphore.getAvailable());

        semaphore.acquire(3);
        assertEquals(softLimit - 3, semaphore.getAvailable());

        semaphore.release(3);
        assertEquals(softLimit, semaphore.getAvailable());
    }


    @Test
    void normalTasksRespectSoftLimit() throws Exception {
        AtomicInteger concurrentCost = new AtomicInteger(0);
        AtomicInteger maxObserved = new AtomicInteger(0);
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            List<Callable<@Nullable Void>> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                tasks.add(() -> {
                    semaphore.acquire(3);
                    int now = concurrentCost.addAndGet(3);
                    maxObserved.updateAndGet(m -> Math.max(m, now));
                    Thread.sleep(50);
                    concurrentCost.addAndGet(-3);
                    semaphore.release(3);
                    return null;
                });
            }

            pool.invokeAll(tasks);
        }

        assertTrue(maxObserved.get() <= 10);
    }


    @Test
    void oversizedTaskWaitsForDrainAndRunsAlone() throws Exception {
        assertEquals(10, semaphore.getAvailable());
        assertEquals(10, semaphore.getCurrentLimit());
        assertEquals(0, semaphore.getUsed());
        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            CountDownLatch normalStarted = new CountDownLatch(2);
            CountDownLatch normalCanFinish = new CountDownLatch(1);

            for (int i = 0; i < 2; i++) {
                pool.submit(() -> {
                    semaphore.acquire(5);
                    normalStarted.countDown();
                    normalCanFinish.await();
                    semaphore.release(5);
                    return null;
                });
            }

            normalStarted.await();

            AtomicInteger duringOversized = new AtomicInteger(0);

            Future<?> oversized = pool.submit(() -> {
                semaphore.acquire(25);
                duringOversized.set(semaphore.getUsed());
                semaphore.release(25);
                return null;
            });

            Thread.sleep(50);
            assertEquals(10, semaphore.getUsed());

            normalCanFinish.countDown();
            oversized.get(2, TimeUnit.SECONDS);

            // Thread.sleep(1000);
            assertEquals(10, semaphore.getAvailable());
            assertEquals(25, duringOversized.get());
            assertEquals(10, semaphore.getCurrentLimit());
            assertEquals(0, semaphore.getUsed());
        }
    }


    @Test
    void oversizedBlocksNewNormalTasks() throws Exception {
        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            semaphore.acquire(10);

            Future<?> oversized = pool.submit(() -> {
                semaphore.acquire(20);
                semaphore.release(20);
                return null;
            });

            Thread.sleep(50);
            AtomicInteger ran = new AtomicInteger(0);

            Future<?> normal = pool.submit(() -> {
                semaphore.acquire(1);
                ran.incrementAndGet();
                semaphore.release(1);
                return null;
            });

            semaphore.release(10);
            oversized.get(2, TimeUnit.SECONDS);

            normal.get(2, TimeUnit.SECONDS);
            assertEquals(1, ran.get());
        }
    }


    @Test
    void multipleOversizedRequestsSerialized() throws InterruptedException {
        int numOversized = 3;
        CountDownLatch allComplete = new CountDownLatch(numOversized);
        List<Long> startTimes = new CopyOnWriteArrayList<>();
        List<Long> endTimes = new CopyOnWriteArrayList<>();

        for (int i = 0; i < numOversized; i++) {
            new Thread(() -> {
                try {
                    semaphore.acquire(15);
                    startTimes.add(System.currentTimeMillis());
                    Thread.sleep(300);
                    endTimes.add(System.currentTimeMillis());
                    semaphore.release(15);
                    allComplete.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        assertTrue(allComplete.await(5, TimeUnit.SECONDS));
        assertEquals(numOversized, startTimes.size());
        assertEquals(numOversized, endTimes.size());

        for (int i = 0; i < numOversized - 1; i++) {
            assertTrue(endTimes.get(i) <= startTimes.get(i + 1) + 50,
                    "Oversized tasks should not overlap");
        }
    }


    @Test
    void testStressTestMixedRequests() throws InterruptedException {
        int numNormal = 20;
        int numOversized = 3;
        CountDownLatch allComplete = new CountDownLatch(numNormal + numOversized);
        try (ExecutorService executor = Executors.newFixedThreadPool(15)) {
            for (int i = 0; i < numNormal; i++) {
                executor.submit(() -> {
                    try {
                        semaphore.acquire(2);
                        Thread.sleep(50);
                        semaphore.release(2);
                        allComplete.countDown();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            for (int i = 0; i < numOversized; i++) {
                executor.submit(() -> {
                    try {
                        semaphore.acquire(12);
                        Thread.sleep(100);
                        semaphore.release(12);
                        allComplete.countDown();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }

        assertTrue(allComplete.await(10, TimeUnit.SECONDS));

        Thread.sleep(100);
        assertEquals(softLimit, semaphore.getAvailable());
    }


    @Test
    void testExactSoftLimitIsTreatedAsNormal() throws InterruptedException {
        CountDownLatch acquired = new CountDownLatch(1);

        Thread thread = new Thread(() -> {
            try {
                semaphore.acquire(softLimit);
                acquired.countDown();
                Thread.sleep(200);
                semaphore.release(softLimit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        thread.start();

        assertTrue(acquired.await(1, TimeUnit.SECONDS));
        assertEquals(0, semaphore.getAvailable());

        thread.join();
        assertEquals(softLimit, semaphore.getAvailable());
    }



    // -------------------------------------------------------------------------
    // Two small requests at softLimit can coexist (they are both "small")
    // -------------------------------------------------------------------------

    @Test
    void twoSmallRequestsAtSoftLimitCanCoexist() throws Exception {
        // acquire(softLimit) is small, so two such requests each holding half
        // should NOT be possible — only one fits. But two acquire(5)s should both fit.
        CountDownLatch bothAcquired = new CountDownLatch(2);

        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            CountDownLatch canRelease = new CountDownLatch(1);

            for (int i = 0; i < 2; i++) {
                pool.submit(() -> {
                    semaphore.acquire(5);
                    bothAcquired.countDown();
                    canRelease.await();
                    semaphore.release(5);
                    return null;
                });
            }

            // Both small requests should be able to hold concurrently (5+5 == softLimit).
            assertTrue(bothAcquired.await(1, TimeUnit.SECONDS),
                    "Two acquire(5) calls should both succeed concurrently under softLimit=10");
            assertEquals(10, semaphore.getUsed());
            assertEquals(0, semaphore.getAvailable());

            canRelease.countDown();
        }

        assertEquals(softLimit, semaphore.getAvailable());
    }


    // -------------------------------------------------------------------------
    // getCurrentLimit and getAvailable reflect expansion during a big request
    // -------------------------------------------------------------------------

    @Test
    void currentLimitExpandsDuringBigRequest() throws Exception {
        CountDownLatch bigAcquired = new CountDownLatch(1);
        CountDownLatch canRelease  = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            pool.submit(() -> {
                semaphore.acquire(25);
                bigAcquired.countDown();
                canRelease.await();
                semaphore.release(25);
                return null;
            });

            assertTrue(bigAcquired.await(2, TimeUnit.SECONDS));

            // While the big request holds its permits the limit must be expanded.
            assertEquals(25, semaphore.getCurrentLimit());
            assertEquals(25, semaphore.getUsed());
            assertEquals(0, semaphore.getAvailable()); // limit - used = 35 - 25 = 10

            canRelease.countDown();
        }

        // After release everything must be back to normal.
        assertEquals(softLimit, semaphore.getCurrentLimit());
        assertEquals(0, semaphore.getUsed());
        assertEquals(softLimit, semaphore.getAvailable());
    }


    // -------------------------------------------------------------------------
    // Consecutive big requests: second truly waits for first to *release*
    // -------------------------------------------------------------------------

    @Test
    void consecutiveBigRequestsDoNotOverlap() throws Exception {
        // Track whether both big requests ever held permits simultaneously.
        AtomicInteger concurrentBig = new AtomicInteger(0);
        AtomicInteger maxConcurrentBig = new AtomicInteger(0);

        CountDownLatch first  = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            Future<?> f1 = pool.submit(() -> {
                semaphore.acquire(15);
                concurrentBig.incrementAndGet();
                maxConcurrentBig.updateAndGet(m -> Math.max(m, concurrentBig.get()));
                first.countDown();
                Thread.sleep(200);
                concurrentBig.decrementAndGet();
                semaphore.release(15);
                return null;
            });

            // Give first a head-start so it definitely acquires before the second submits.
            assertTrue(first.await(2, TimeUnit.SECONDS));

            Future<?> f2 = pool.submit(() -> {
                semaphore.acquire(20);
                concurrentBig.incrementAndGet();
                maxConcurrentBig.updateAndGet(m -> Math.max(m, concurrentBig.get()));
                second.countDown();
                Thread.sleep(200);
                concurrentBig.decrementAndGet();
                semaphore.release(20);
                return null;
            });

            f1.get(3, TimeUnit.SECONDS);
            assertTrue(second.await(3, TimeUnit.SECONDS));
            f2.get(3, TimeUnit.SECONDS);
        }

        assertEquals(1, maxConcurrentBig.get(), "Big requests must never overlap");
        assertEquals(softLimit, semaphore.getAvailable());
    }


    // -------------------------------------------------------------------------
    // Interrupt while draining: big slot is released, semaphore stays healthy
    // -------------------------------------------------------------------------

    @Test
    void interruptWhileDrainingReleasesBigSlot() throws Exception {
        // Hold all permits so the big request will block in the drain phase.
        semaphore.acquire(10);

        Thread bigThread = new Thread(() -> {
            try {
                semaphore.acquire(20); // will block — used > 0
                semaphore.release(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // swallow, test will verify aftermath
            }
        });
        bigThread.start();

        Thread.sleep(50); // let bigThread enter acquire and block
        bigThread.interrupt();
        bigThread.join(1000);
        assertFalse(bigThread.isAlive(), "Interrupted thread should have exited");

        // Release the small permits that were blocking the drain.
        semaphore.release(10);

        // The semaphore must be fully operational: another big request should succeed.
        CountDownLatch done = new CountDownLatch(1);
        Thread recovery = new Thread(() -> {
            try {
                semaphore.acquire(20);
                semaphore.release(20);
                done.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        recovery.start();

        assertTrue(done.await(2, TimeUnit.SECONDS),
                "Semaphore should be operational after interrupted big request");
        assertEquals(softLimit, semaphore.getAvailable());
    }


    // -------------------------------------------------------------------------
    // Interrupt while waiting for the big slot: semaphore stays healthy
    // -------------------------------------------------------------------------

    @Test
    void interruptWhileWaitingForBigSlotLeavesOtherBigRequestUnaffected() throws Exception {
        CountDownLatch firstBigRunning = new CountDownLatch(1);
        CountDownLatch firstBigCanFinish = new CountDownLatch(1);

        // First big request acquires and holds.
        Thread first = new Thread(() -> {
            try {
                semaphore.acquire(15);
                firstBigRunning.countDown();
                firstBigCanFinish.await();
                semaphore.release(15);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        first.start();
        assertTrue(firstBigRunning.await(2, TimeUnit.SECONDS));

        // Second big request queues behind the first — it will block on bigSlotAvailable.
        Thread second = new Thread(() -> {
            try {
                semaphore.acquire(20);
                semaphore.release(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // expected
            }
        });
        second.start();
        Thread.sleep(50); // let second block on bigSlotAvailable

        // Interrupt the second — it should exit without corrupting the semaphore.
        second.interrupt();
        second.join(1000);
        assertFalse(second.isAlive());

        // Let the first finish; semaphore must be clean afterwards.
        firstBigCanFinish.countDown();
        first.join(1000);

        assertEquals(softLimit, semaphore.getAvailable());
        assertEquals(softLimit, semaphore.getCurrentLimit());
        assertEquals(0,         semaphore.getUsed());
    }


    // -------------------------------------------------------------------------
    // release() more than acquired throws IllegalStateException
    // -------------------------------------------------------------------------

    @Test
    void releasingMoreThanAcquiredThrows() throws InterruptedException {
        semaphore.acquire(3);
        assertThrows(IllegalStateException.class, () -> semaphore.release(5));
        // Clean up so other tests are unaffected.
        semaphore.release(3);
    }

    @Test
    void releasingWithoutAcquiringThrows() {
        assertThrows(IllegalStateException.class, () -> semaphore.release(1));
    }


    // -------------------------------------------------------------------------
    // toString reflects both used and available correctly
    // -------------------------------------------------------------------------

    @Test
    void toStringReflectsState() throws InterruptedException {
        semaphore.acquire(4);
        String s = semaphore.toString();
        assertTrue(s.contains("used=4"),      "toString must contain used=4, got: " + s);
        assertTrue(s.contains("available=6"), "toString must contain available=6, got: " + s);
        semaphore.release(4);

        String idle = semaphore.toString();
        assertTrue(idle.contains("used=0"),       "toString must contain used=0, got: " + idle);
        assertTrue(idle.contains("available=10"), "toString must contain available=10, got: " + idle);
    }

    @Test
    void toStringReflectsExpandedStateDuringBigRequest() throws Exception {
        CountDownLatch bigAcquired = new CountDownLatch(1);
        CountDownLatch canRelease  = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            pool.submit(() -> {
                semaphore.acquire(25);
                bigAcquired.countDown();
                canRelease.await();
                semaphore.release(25);
                return null;
            });

            assertTrue(bigAcquired.await(2, TimeUnit.SECONDS));

            String s = semaphore.toString();
            assertTrue(s.contains("used=25"), s);
            assertTrue(s.contains("available=0"), s);
            assertTrue(s.contains("currentLimit=25"), s);
            assertTrue(s.contains("softLimit=10"), s);

            canRelease.countDown();
        }
    }


    // -------------------------------------------------------------------------
    // Argument validation
    // -------------------------------------------------------------------------

    @Test
    void acquireZeroPermitsThrows() {
        assertThrows(IllegalArgumentException.class, () -> semaphore.acquire(0));
    }

    @Test
    void releaseZeroPermitsThrows() {
        assertThrows(IllegalArgumentException.class, () -> semaphore.release(0));
    }

    @Test
    void constructorRejectsZeroSoftLimit() {
        assertThrows(IllegalArgumentException.class, () -> new DynamicSemaphore(0));
    }
}
