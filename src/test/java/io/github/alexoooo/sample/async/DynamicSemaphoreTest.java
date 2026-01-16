package io.github.alexoooo.sample.async;

import io.github.alexoooo.sample.async.generic.DynamicSemaphore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


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
                semaphore.acquire(25); // > softLimit
                duringOversized.set(semaphore.getUsed());
                semaphore.release(25);
                return null;
            });

            Thread.sleep(50);
            assertEquals(10, semaphore.getUsed());

            normalCanFinish.countDown();
            oversized.get(2, TimeUnit.SECONDS);

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
}
