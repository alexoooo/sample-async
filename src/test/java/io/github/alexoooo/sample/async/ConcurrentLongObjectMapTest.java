package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.generic.ConcurrentLongObjectMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.*;


public class ConcurrentLongObjectMapTest {
    //-----------------------------------------------------------------------------------------------------------------
    private ConcurrentLongObjectMap<String> map;

    @BeforeEach
    public void setUp() {
        map = new ConcurrentLongObjectMap<>();
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void putAndGet() {
        assertNull(map.get(1L));

        map.put(1L, "value1");
        assertEquals("value1", map.get(1L));

        map.put(2L, "value2");
        assertEquals("value2", map.get(2L));
        assertEquals("value1", map.get(1L));
    }

    @Test
    public void putOverwrite() {
        map.put(1L, "value1");
        assertEquals("value1", map.get(1L));

        String old = map.put(1L, "value2");
        assertEquals("value1", old);
        assertEquals("value2", map.get(1L));
    }


    @Test
    void remove() {
        map.put(1L, "value1");
        map.put(2L, "value2");

        String removed = map.remove(1L);
        assertEquals("value1", removed);
        assertNull(map.get(1L));
        assertEquals("value2", map.get(2L)); // Other value still there

        assertNull(map.remove(999L)); // Remove non-existent key
    }

    @Test
    void containsKey() {
        assertFalse(map.containsKey(1L));

        map.put(1L, "value1");
        assertTrue(map.containsKey(1L));
        assertFalse(map.containsKey(2L));

        map.remove(1L);
        assertFalse(map.containsKey(1L));
    }


    @Test
    void size() {
        assertEquals(0, map.size());

        map.put(1L, "value1");
        assertEquals(1, map.size());

        map.put(2L, "value2");
        map.put(3L, "value3");
        assertEquals(3, map.size());

        map.put(2L, "updated");
        assertEquals(3, map.size());

        map.remove(1L);
        assertEquals(2, map.size());
    }


    @Test
    void clear() {
        map.put(1L, "value1");
        map.put(2L, "value2");
        map.put(3L, "value3");
        assertEquals(3, map.size());

        map.clear();
        assertEquals(0, map.size());
        assertNull(map.get(1L));
        assertNull(map.get(2L));
        assertNull(map.get(3L));
    }


    @Test
    void computeIfAbsent() {
        AtomicInteger computeCount = new AtomicInteger(0);

        String result1 = map.computeIfAbsent(1L, key -> {
            computeCount.incrementAndGet();
            return "computed-" + key;
        });

        assertEquals("computed-1", result1);
        assertEquals(1, computeCount.get());

        String result2 = map.computeIfAbsent(1L, key -> {
            computeCount.incrementAndGet();
            return "computed-again-" + key;
        });

        assertEquals("computed-1", result2);
        assertEquals(1, computeCount.get());
    }


    @Test
    void putIfAbsent() {
        String result1 = map.putIfAbsent(1L, "value1");
        assertNull(result1);
        assertEquals("value1", map.get(1L));

        String result2 = map.putIfAbsent(1L, "value2");
        assertEquals("value1", result2);
        assertEquals("value1", map.get(1L));
    }


    @Test
    void putIfAbsentMultipleKeys() {
        assertNull(map.putIfAbsent(1L, "value1"));
        assertNull(map.putIfAbsent(2L, "value2"));
        assertNull(map.putIfAbsent(3L, "value3"));

        assertEquals("value1", map.putIfAbsent(1L, "newvalue1"));
        assertEquals("value2", map.putIfAbsent(2L, "newvalue2"));
        assertEquals("value3", map.putIfAbsent(3L, "newvalue3"));

        assertEquals("value1", map.get(1L));
        assertEquals("value2", map.get(2L));
        assertEquals("value3", map.get(3L));
    }


    @Test
    void putIfAbsentAfterRemove() {
        map.put(1L, "value1");
        assertEquals("value1", map.get(1L));

        map.remove(1L);
        assertNull(map.get(1L));

        assertNull(map.putIfAbsent(1L, "value2"));
        assertEquals("value2", map.get(1L));
    }


    @Test
    void putVsPutIfAbsent() {
        map.putIfAbsent(1L, "value1");
        assertEquals("value1", map.get(1L));

        map.put(1L, "value2");
        assertEquals("value2", map.get(1L));

        map.putIfAbsent(1L, "value3");
        assertEquals("value2", map.get(1L));
    }


    @Test
    void multipleStripes() {
        for (long i = 0; i < 1000; i++) {
            map.put(i, "value-" + i);
        }
        assertEquals(1000, map.size());
        for (long i = 0; i < 1000; i++) {
            assertEquals("value-" + i, map.get(i));
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @RepeatedTest(10)
    void concurrentPuts() throws InterruptedException {
        int numThreads = 10;
        int operationsPerThread = 1000;
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch latch = new CountDownLatch(numThreads);
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            long key = threadId * operationsPerThread + i;
                            map.put(key, "thread-" + threadId + "-value-" + i);
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        assertEquals(numThreads * operationsPerThread, map.size());

        for (int t = 0; t < numThreads; t++) {
            for (int i = 0; i < operationsPerThread; i++) {
                long key = t * operationsPerThread + i;
                String expected = "thread-" + t + "-value-" + i;
                assertEquals(expected, map.get(key));
            }
        }
    }


    @RepeatedTest(10)
    void concurrentReadsAndWrites() throws InterruptedException {
        int numReaders = 8;
        int numWriters = 4;
        int operations = 10000;

        for (long i = 0; i < 100; i++) {
            map.put(i, "initial-" + i);
        }

        AtomicInteger readErrors = new AtomicInteger(0);
        try (ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters)) {
            CountDownLatch latch = new CountDownLatch(numReaders + numWriters);

            for (int t = 0; t < numReaders; t++) {
                executor.submit(() -> {
                    try {
                        RandomGenerator random = new SplittableRandom();
                        for (int i = 0; i < operations; i++) {
                            long key = random.nextInt(100);
                            String value = map.get(key);
                            if (value != null && !value.startsWith("initial-") && !value.startsWith("updated-")) {
                                readErrors.incrementAndGet();
                            }
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            for (int t = 0; t < numWriters; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        RandomGenerator random = new SplittableRandom();
                        for (int i = 0; i < operations; i++) {
                            long key = random.nextInt(100);
                            map.put(key, "updated-" + threadId + "-" + i);
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }

        assertEquals(0, readErrors.get(), "Readers should never see corrupted data");
        assertEquals(100, map.size());
    }


    @RepeatedTest(10)
    void concurrentComputeIfAbsent() throws InterruptedException {
        int numThreads = 20;
        int numKeys = 100;
        AtomicInteger computeCount = new AtomicInteger(0);
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch latch = new CountDownLatch(numThreads);
            for (int t = 0; t < numThreads; t++) {
                executor.submit(() -> {
                    try {
                        RandomGenerator random = new SplittableRandom();
                        for (int i = 0; i < 1000; i++) {
                            long key = random.nextInt(numKeys);
                            map.computeIfAbsent(key, k -> {
                                computeCount.incrementAndGet();
                                try {
                                    Thread.sleep(0, 100);
                                }
                                catch (InterruptedException e) {
                                    throw new IllegalStateException(e);
                                }
                                return "computed-" + k;
                            });
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }
        assertEquals(numKeys, computeCount.get(),
                "Each key should be computed exactly once");
        assertEquals(numKeys, map.size());

        for (long i = 0; i < numKeys; i++) {
            assertEquals("computed-" + i, map.get(i));
        }
    }


    @RepeatedTest(10)
    void concurrentRemoves() throws InterruptedException {
        int numElements = 1000;
        int numThreads = 10;

        for (long i = 0; i < numElements; i++) {
            map.put(i, "value-" + i);
        }

        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch latch = new CountDownLatch(numThreads);
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = threadId; i < numElements; i += numThreads) {
                            map.remove(i);
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }

        assertEquals(0, map.size());
        for (long i = 0; i < numElements; i++) {
            assertNull(map.get(i));
        }
    }


    @RepeatedTest(10)
    void concurrentClearAndOperations() throws InterruptedException {
        int numThreads = 8;
        AtomicLong successfulOps = new AtomicLong(0);
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch latch = new CountDownLatch(numThreads);

            for (int t = 0; t < numThreads - 1; t++) {
                executor.submit(() -> {
                    try {
                        RandomGenerator random = new SplittableRandom();
                        for (int i = 0; i < 5000; i++) {
                            long key = random.nextInt(100);
                            if (random.nextBoolean()) {
                                map.put(key, "value-" + key);
                            }
                            else {
                                map.get(key);
                            }
                            successfulOps.incrementAndGet();
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            executor.submit(() -> {
                try {
                    for (int i = 0; i < 10; i++) {
                        Thread.sleep(50);
                        map.clear();
                    }
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                finally {
                    latch.countDown();
                }
            });

            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }

        assertTrue(successfulOps.get() > 0);
    }


    @RepeatedTest(10)
    void contentionOnSameKeys() throws InterruptedException {
        int numThreads = 20;
        int numKeys = 10;
        int operationsPerThread = 1000;
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            CountDownLatch latch = new CountDownLatch(numThreads);
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        RandomGenerator random = new SplittableRandom();
                        for (int i = 0; i < operationsPerThread; i++) {
                            long key = random.nextInt(numKeys);

                            switch (random.nextInt(4)) {
                                case 0:
                                    map.put(key, "thread-" + threadId + "-op-" + i);
                                    break;
                                case 1:
                                    map.get(key);
                                    break;
                                case 2:
                                    map.computeIfAbsent(key, k -> "computed-" + threadId);
                                    break;
                                case 3:
                                    map.remove(key);
                                    break;
                            }
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }

        assertTrue(map.size() <= numKeys);
    }


    @Test
    void allOperations() throws InterruptedException {
        int numThreads = 16;
        int duration = 5;
        AtomicInteger errors = new AtomicInteger(0);
        try (ExecutorService executor = Executors.newFixedThreadPool(numThreads)) {
            AtomicBoolean stop = new AtomicBoolean();
            CountDownLatch latch = new CountDownLatch(numThreads);

            for (int t = 0; t < numThreads; t++) {
                executor.submit(() -> {
                    try {
                        RandomGenerator random = new SplittableRandom();
                        while (!stop.get()) {
                            try {
                                long key = random.nextInt(1000);
                                int op = random.nextInt(10);

                                if (op < 5) {
                                    map.get(key);
                                }
                                else if (op < 6) {
                                    map.put(key, "value-" + key);
                                }
                                else if (op < 7) {
                                    map.putIfAbsent(key, "value-" + key);
                                }
                                else if (op < 8) {
                                    map.computeIfAbsent(key, k -> "computed-" + k);
                                }
                                else if (op < 9) {
                                    map.remove(key);
                                }
                                else {
                                    map.containsKey(key);
                                }
                            }
                            catch (Exception e) {
                                errors.incrementAndGet();
                            }
                        }
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            Thread.sleep(duration * 1000);
            stop.set(true);

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        assertEquals(0, errors.get());

        map.put(999999L, "test");
        assertEquals("test", map.get(999999L));
    }
}
