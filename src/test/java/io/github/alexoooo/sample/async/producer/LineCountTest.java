package io.github.alexoooo.sample.async.producer;

import io.github.alexoooo.sample.async.generic.io.FileChunk;
import io.github.alexoooo.sample.async.generic.io.FileLineCounter;
import io.github.alexoooo.sample.async.generic.io.FileReaderPooledProducer;
import io.github.alexoooo.sample.async.generic.io.FileReaderProducer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class LineCountTest
{
    //-----------------------------------------------------------------------------------------------------------------
    @Test
//    @RepeatedTest(100)
    public void countLines() {
        int lineCount = 999;
        Supplier<InputStream> lines = generateLines(lineCount);
        try (FileReaderProducer reader = FileReaderProducer.createStarted(
                lines, 128, 16);
             FileLineCounter counter = FileLineCounter.createStarted(2)
        ) {
            List<FileChunk> buffer = new ArrayList<>();
            while (true) {
                boolean hasNext = reader.poll(buffer);
                for (int i = 0; i < buffer.size();) {
                    i += counter.offer(buffer, i);
                }
                buffer.clear();
                if (! hasNext) {
                    break;
                }
            }
            counter.awaitDoneWork();
            assertEquals(lineCount, counter.lineCount());
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @RepeatedTest(10)
    public void countBytesPooled() {
        int byteCount = 999;
        Supplier<InputStream> lines = generateBytes(byteCount);
        try (FileReaderPooledProducer reader = FileReaderPooledProducer.createStarted(
                lines, 32, 16);
             FileLineCounter counter = FileLineCounter.createStarted(2)
        ) {
            long total = 0;
            while (true) {
                AsyncResult<FileChunk> result = reader.poll();

                FileChunk value = result.value();
                if (value != null) {
                    counter.put(value);
                    counter.awaitDoneWork();

                    total += value.length;
                    reader.release(value);
                }

                if (result.endReached()) {
                    break;
                }
            }

            counter.awaitDoneWork();
            assertEquals(byteCount, counter.byteCount());
            assertEquals(byteCount, total);
        }
    }


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
