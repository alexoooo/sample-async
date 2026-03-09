package io.github.alexoooo.sample.async.util;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;


public class ConcurrentDoubleSumTest
{
    //-----------------------------------------------------------------------------------------------------------------
    private ConcurrentDoubleSum sum;

    @BeforeEach
    void setUp() {
        sum = new ConcurrentDoubleSum();
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void initialSumIsZero() {
        assertEquals(0.0, sum.sum());
    }

    @Test
    void singlePositiveValue() {
        sum.add(42.5);
        assertEquals(42.5, sum.sum());
    }

    @Test
    void singleNegativeValue() {
        sum.add(-17.3);
        assertEquals(-17.3, sum.sum(), 1e-15);
    }

    @Test
    void addZeroHasNoEffect() {
        sum.add(1.0);
        sum.add(0.0);
        assertEquals(1.0, sum.sum());
    }

    @Test
    void sumOfPositiveAndNegativeIsZero() {
        sum.add(1.0);
        sum.add(-1.0);
        assertEquals(0.0, sum.sum(), 1e-15);
    }

    @Test
    void integerValuesExact() {
        for (int i = 1; i <= 100; i++) sum.add(i);
        assertEquals(5050.0, sum.sum(), 1e-10);
    }

    @Test
    void addInfinity() {
        sum.add(Double.POSITIVE_INFINITY);
        assertEquals(Double.POSITIVE_INFINITY, sum.sum());
    }

    @Test
    void addNegativeInfinity() {
        sum.add(Double.NEGATIVE_INFINITY);
        assertEquals(Double.NEGATIVE_INFINITY, sum.sum());
    }

    @Test
    void addNaNPropagates() {
        sum.add(1.0);
        sum.add(Double.NaN);
        assertTrue(Double.isNaN(sum.sum()));
    }

    @Test
    void addPositiveAndNegativeInfinityProducesNaN() {
        sum.add(Double.POSITIVE_INFINITY);
        sum.add(Double.NEGATIVE_INFINITY);
        assertTrue(Double.isNaN(sum.sum()));
    }

    @Test
    void sumOfEmptyIsZeroAfterReset() {
        sum.add(99.9);
        sum.reset();
        assertEquals(0.0, sum.sum());
    }

    @Test
    void resetAndReuse() {
        sum.add(100.0);
        sum.reset();
        sum.add(7.0);
        assertEquals(7.0, sum.sum());
    }

    @Test
    void addMaxDouble() {
        sum.add(Double.MAX_VALUE);
        assertEquals(Double.MAX_VALUE, sum.sum());
    }

    @Test
    void addMaxDoubleTwiceOverflows() {
        sum.add(Double.MAX_VALUE);
        sum.add(Double.MAX_VALUE);
        assertEquals(Double.POSITIVE_INFINITY, sum.sum());
    }

    @Test
    void addMinDoubleNegative() {
        sum.add(-Double.MAX_VALUE);
        assertEquals(-Double.MAX_VALUE, sum.sum());
    }

    @Test
    void largeNumberOfAdds() {
        int n = 1_000_000;
        for (int i = 0; i < n; i++) sum.add(1.0);
        assertEquals(n, sum.sum(), 1e-6);
    }

    @Test
    void alternatingLargeAndSmallPreservesSmall() {
        sum.add(1e17);
        sum.add(1.0);
        sum.add(-1e17);
        assertEquals(1.0, sum.sum(), 0.5);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    void neumaierCompensationLargeSmallLarge() {
        sum.add(1e16);
        sum.add(1.0);
        sum.add(-1e16);
        assertEquals(1.0, sum.sum(), 1e-6);
    }

    @Test
    void compensatedSumOfManyTenths() {
        for (int i = 0; i < 1000; i++) sum.add(0.1);
        assertEquals(100.0, sum.sum(), 1e-10);
    }

    @Test
    void precisionBetterThanNaive() {
        double big = 1e15;
        double small = 0.1;
        int pairs = 1000;

        double naiveSum = 0.0;
        for (int i = 0; i < pairs; i++) {
            naiveSum += big;
            naiveSum += (-big + small);
        }

        for (int i = 0; i < pairs; i++) {
            sum.add(big);
            sum.add(-big + small);
        }

        double expected = pairs * small;  // 100.0
        double neumaierError = Math.abs(sum.sum() - expected);
        double naiveError    = Math.abs(naiveSum  - expected);

        assertTrue(neumaierError <= naiveError);
    }

    @Test
    void sumOfSquares() {
        int n = 10_000;
        for (int i = 1; i <= n; i++) sum.add((double) i * i);
        double expected = (double) n * (n + 1) * (2L * n + 1) / 6.0;
        assertEquals(expected, sum.sum(), expected * 1e-10);
    }

    @Test
    void subnormalValues() {
        double tiny = Double.MIN_VALUE;
        int count = 1000;
        for (int i = 0; i < count; i++) sum.add(tiny);
        assertTrue(sum.sum() > 0.0);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    @Timeout(10)
    void concurrentAddsProduceCorrectSum() throws InterruptedException {
        int threads = 8;
        int addsPerThread = 100_000;
        double valuePerAdd = 1.0;
        double expected = threads * addsPerThread * valuePerAdd;

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try { barrier.await(); } catch (Exception ignored) {}
                    for (int i = 0; i < addsPerThread; i++) sum.add(valuePerAdd);
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(9, TimeUnit.SECONDS));
        }

        assertEquals(expected, sum.sum(), expected * 1e-9);
    }

    @Test
    @Timeout(10)
    void concurrentAddsWithMixedSignsConvergeToZero() throws InterruptedException {
        int threads = 8;
        int addsPerThread = 50_000;

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            for (int t = 0; t < threads; t++) {
                final int sign = (t % 2 == 0) ? 1 : -1;
                pool.submit(() -> {
                    try { barrier.await(); } catch (Exception ignored) {}
                    for (int i = 0; i < addsPerThread; i++) sum.add(sign * 1.0);
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(9, TimeUnit.SECONDS));
        }

        assertEquals(0.0, sum.sum(), 1e-6);
    }

    @Test
    @Timeout(15)
    void concurrentAddsAndSumCallsDontDeadlock() throws InterruptedException {
        int writers = 4;
        int readers = 4;
        int durationSeconds = 2;

        try (ExecutorService pool = Executors.newFixedThreadPool(writers + readers)) {
            AtomicBoolean running = new AtomicBoolean(true);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < writers; i++) {
                futures.add(pool.submit(() -> {
                    while (running.get()) sum.add(1.0);
                }));
            }
            for (int i = 0; i < readers; i++) {
                futures.add(pool.submit(() -> {
                    while (running.get()) {
                        double s = sum.sum();
                        assertTrue(s >= 0.0);
                    }
                }));
            }

            Thread.sleep(durationSeconds * 1000L);
            running.set(false);
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

            for (Future<?> f : futures) {
                assertDoesNotThrow(() -> f.get(1, TimeUnit.SECONDS));
            }
        }
    }

    @RepeatedTest(5)
    @Timeout(10)
    void concurrentSumMatchesSequentialSum() throws InterruptedException {
        int threads = 6;
        int addsPerThread = 10_000;

        // Generate deterministic values per thread
        double[][] values = new double[threads][addsPerThread];
        double sequentialSum = 0.0;
        for (int t = 0; t < threads; t++)
            for (int i = 0; i < addsPerThread; i++) {
                values[t][i] = (t + 1) * 0.001 * (i + 1);
                sequentialSum += values[t][i];
            }

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try { barrier.await(); } catch (Exception ignored) {}
                    for (double v : values[tid]) sum.add(v);
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(9, TimeUnit.SECONDS));
        }

        // Compensated concurrent sum should be at least as accurate as naive sequential
        assertEquals(sequentialSum, sum.sum(), Math.abs(sequentialSum) * 1e-9);
    }

    @SuppressWarnings("BusyWait")
    @Test
    @Timeout(10)
    void resetUnderConcurrencyDoesNotCauseException() throws InterruptedException {
        int writers = 4;
        int duration = 2;
        try (ExecutorService pool = Executors.newFixedThreadPool(writers + 1)) {
            AtomicBoolean running = new AtomicBoolean(true);

            for (int i = 0; i < writers; i++) {
                pool.submit(() -> { while (running.get()) sum.add(1.0); });
            }
            pool.submit(() -> {
                while (running.get()) {
                    sum.reset();
                    try { Thread.sleep(10); } catch (InterruptedException ignored) {}
                }
            });
            Thread.sleep(duration * 1000L);
            running.set(false);
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    @Timeout(10)
    void threeIndependentInstancesAreIsolated() throws InterruptedException {
        ConcurrentDoubleSum sumA = new ConcurrentDoubleSum();
        ConcurrentDoubleSum sumB = new ConcurrentDoubleSum();
        ConcurrentDoubleSum sumC = new ConcurrentDoubleSum();

        int threads = 6;
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try { barrier.await(); } catch (Exception ignored) {}
                    for (int i = 0; i < 10_000; i++) {
                        sumA.add(1.0);
                        sumB.add(2.0);
                        sumC.add(3.0);
                    }
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(9, TimeUnit.SECONDS));
        }

        double expected = threads * 10_000.0;
        assertEquals(expected, sumA.sum(), expected * 1e-9);
        assertEquals(expected * 2.0, sumB.sum(), expected * 1e-9);
        assertEquals(expected * 3.0, sumC.sum(), expected * 1e-9);
    }
}
