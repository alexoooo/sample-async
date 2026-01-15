package io.github.alexoooo.sample.async;

import io.github.alexoooo.sample.async.generic.DynamicSemaphore;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class DynamicSemaphoreTest {
    @Test
    void normalTasksRespectSoftLimit() throws Exception {
        DynamicSemaphore sem = new DynamicSemaphore(10);

        AtomicInteger concurrentCost = new AtomicInteger(0);
        AtomicInteger maxObserved = new AtomicInteger(0);
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            List<Callable<@Nullable Void>> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                tasks.add(() -> {
                    sem.acquire(3);
                    int now = concurrentCost.addAndGet(3);
                    maxObserved.updateAndGet(m -> Math.max(m, now));
                    Thread.sleep(50);
                    concurrentCost.addAndGet(-3);
                    sem.release(3);
                    return null;
                });
            }

            pool.invokeAll(tasks);
        }

        assertTrue(maxObserved.get() <= 10);
    }


    @Test
    void oversizedTaskWaitsForDrainAndRunsAlone() throws Exception {
        DynamicSemaphore sem = new DynamicSemaphore(10);
        try (ExecutorService pool = Executors.newCachedThreadPool()) {
            CountDownLatch normalStarted = new CountDownLatch(2);
            CountDownLatch normalCanFinish = new CountDownLatch(1);

            // Two normal tasks
            for (int i = 0; i < 2; i++) {
                pool.submit(() -> {
                    sem.acquire(5);
                    normalStarted.countDown();
                    normalCanFinish.await();
                    sem.release(5);
                    return null;
                });
            }

            normalStarted.await();

            AtomicInteger duringOversized = new AtomicInteger(0);

            Future<?> oversized = pool.submit(() -> {
                sem.acquire(25); // > softMax
                duringOversized.set(sem.getUsed());
                sem.release(25);
                return null;
            });

            // Oversized must NOT run yet
            Thread.sleep(50);
            assertEquals(10, sem.getUsed());

            // Let normals finish
            normalCanFinish.countDown();
            oversized.get(2, TimeUnit.SECONDS);

            assertEquals(25, duringOversized.get());
            assertEquals(0, sem.getUsed());
            assertEquals(10, sem.getCurrentLimit());
        }
    }


    @Test
    void oversizedBlocksNewNormalTasks() throws Exception {
        DynamicSemaphore sem = new DynamicSemaphore(10);
        try (ExecutorService pool = Executors.newCachedThreadPool()) {

            sem.acquire(10); // fill capacity

            Future<?> oversized = pool.submit(() -> {
                sem.acquire(20);
                sem.release(20);
                return null;
            });

            Thread.sleep(50); // ensure oversized is queued

            AtomicInteger ran = new AtomicInteger(0);

            Future<?> normal = pool.submit(() -> {
                sem.acquire(1);
                ran.incrementAndGet();
                sem.release(1);
                return null;
            });

            // Free capacity
            sem.release(10);

            oversized.get(2, TimeUnit.SECONDS);

            // Normal should only run AFTER oversized
            normal.get(2, TimeUnit.SECONDS);
            assertEquals(1, ran.get());
        }
    }
}
