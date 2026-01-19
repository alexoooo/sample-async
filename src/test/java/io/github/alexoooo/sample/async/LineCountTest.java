package io.github.alexoooo.sample.async;

import io.github.alexoooo.sample.async.generic.io.FileChunk;
import io.github.alexoooo.sample.async.generic.io.FileLineCounter;
import io.github.alexoooo.sample.async.generic.io.FileReaderPooledProducer;
import io.github.alexoooo.sample.async.generic.io.FileReaderProducer;
import io.github.alexoooo.sample.async.producer.AsyncResult;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.*;


public class LineCountTest
{
    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void countLines() {
        RandomGenerator random = new SplittableRandom();
        int lineCount = random.nextInt(1, 100_000);
        Supplier<InputStream> lines = generateLines(lineCount);
        int chunks = 0;
        int bytes = 0;
        int manualLines = 1;
        try (FileReaderProducer reader = FileReaderProducer.createStarted(
                lines, random.nextInt(1, 1000), random.nextInt(1, 16));
             FileLineCounter counter = FileLineCounter.createStarted(random.nextInt(1, 32))
        ) {
            boolean endReached = false;
            List<FileChunk> buffer = new ArrayList<>();
            while (true) {
                boolean hasNext = reader.poll(buffer);
                if (endReached && hasNext) {
                    AsyncResult<FileChunk> following = reader.poll();
                    assertNull(following.value());
                }

                chunks += buffer.size();
                for (FileChunk fileChunk : buffer) {
                    bytes += fileChunk.length;
                    for (int i = 0, s = fileChunk.length; i < s; i++) {
                        if (fileChunk.bytes[i] == '\n') {
                            manualLines++;
                        }
                    }
                }
                for (int i = 0; i < buffer.size();) {
                    i += counter.offer(buffer, i);
                }
                buffer.clear();
                if (!hasNext) {
                    assertTrue(reader.isDone());
                    break;
                }
                if (reader.isDone()) {
                    endReached = true;
                }
            }
            counter.awaitDoneWork();
            assertEquals(lineCount, counter.lineCount());
            assertEquals(lineCount, manualLines);
            assertEquals(chunks, reader.getTotalChunks());
            assertEquals(bytes, reader.getTotalRead());
        }
    }


//    @RepeatedTest(1_000)
//    public void countLinesRepeat() {
//        for (int i = 0; i < 1_000; i++) {
//            countLines();
//        }
//    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void countBytesPooled() {
        RandomGenerator random = new SplittableRandom();
        int byteCount = random.nextInt(1, 50_000);
        Supplier<InputStream> lines = generateBytes(byteCount);
        long total = 0;
        int chunks = 0;

        boolean endReached = false;
        try (FileReaderPooledProducer reader = FileReaderPooledProducer.createStarted(
                lines, random.nextInt(1, 1000), random.nextInt(1, 16));
             FileLineCounter counter = FileLineCounter.createStarted(random.nextInt(1, 64))
        ) {
            while (true) {
                AsyncResult<FileChunk> result = reader.poll();
                if (endReached) {
                    assertTrue(result.endReached());
                    assertNull(result.value());
                }

                FileChunk value = result.value();
                if (value != null) {
                    counter.put(value);
                    counter.awaitDoneWork();
                    chunks++;
                    total += value.length;
                    reader.release(value);
                }

                if (result.endReached()) {
                    assertTrue(reader.isDone());
                    break;
                }
                if (reader.isDone()) {
                    endReached = true;
                }
            }
            counter.awaitDoneWork();
            assertEquals(chunks, reader.getTotalChunks());
            assertEquals(byteCount, reader.getTotalRead());
            assertEquals(byteCount, total);
        }
//            assertEquals(byteCount, counter.byteCount());
    }


//    @RepeatedTest(1_000)
//    public void countBytesPooledRepeat() {
//        for (int i = 0; i < 1_000; i++) {
//            countBytesPooled();
//        }
//    }


    //-----------------------------------------------------------------------------------------------------------------
    public Supplier<InputStream> generateLines(int lineCount) {
        RandomGenerator random = new SplittableRandom(42);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int newlineCount = lineCount - 1;
        for (int i = 0; i < newlineCount; i++) {
            if (random.nextBoolean()) {
                out.write('\r');
            }
            out.write('\n');
        }
        byte[] bytes = out.toByteArray();
        return () -> new ByteArrayInputStream(bytes);
    }


    public Supplier<InputStream> generateBytes(int byteCount) {
        RandomGenerator random = new SplittableRandom(42);
        byte[] bytes = new byte[byteCount];
        random.nextBytes(bytes);
        return () -> new ByteArrayInputStream(bytes);
    }
}
