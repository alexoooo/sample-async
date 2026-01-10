package io.github.alexoooo.sample.async.producer;

import io.github.alexoooo.sample.async.generic.io.FileChunk;
import io.github.alexoooo.sample.async.generic.io.FileLineCounter;
import io.github.alexoooo.sample.async.generic.io.FileReaderPooledProducer;
import io.github.alexoooo.sample.async.generic.io.FileReaderProducer;
import org.jetbrains.lincheck.Lincheck;
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
        int chunks = 0;
        int bytes = 0;
        int manualLines = 1;
        try (FileReaderProducer reader = FileReaderProducer.createStarted(
                lines, 128, 16);
             FileLineCounter counter = FileLineCounter.createStarted(2)
        ) {
            List<FileChunk> buffer = new ArrayList<>();
            while (true) {
                boolean hasNext = reader.poll(buffer);
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
                if (! hasNext) {
//                    if (chunks != 12) {
//                        IO.println("foo");
//                    }
                    break;
                }
            }
            counter.awaitDoneWork();
            assertEquals(lineCount, counter.lineCount());
            assertEquals(lineCount, manualLines);
            assertEquals(chunks, reader.getTotalChunks());
            assertEquals(bytes, reader.getTotalRead());
        }
    }


//    @Test
    @RepeatedTest(1_000)
    public void countLinesRepeat() {
        for (int i = 0; i < 1_000; i++) {
            countLines();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
//    @RepeatedTest(10)
    public void countBytesPooled() {
        int byteCount = 999;
        Supplier<InputStream> lines = generateBytes(byteCount);
        long total = 0;
        int chunks = 0;

        FileReaderPooledProducer reader = FileReaderPooledProducer.createStarted(
                lines, 32, 16);
        try (reader
//        try (FileReaderPooledProducer reader = FileReaderPooledProducer.createStarted(
//                lines, 32, 16);
//             FileLineCounter counter = FileLineCounter.createStarted(32)
        ) {
            while (true) {
                AsyncResult<FileChunk> result = reader.poll();

                FileChunk value = result.value();
                if (value != null) {
//                    counter.put(value);
//                    counter.awaitDoneWork();
                    chunks++;
                    total += value.length;
                    reader.release(value);
                }

                if (result.endReached()) {
                    break;
                }
            }
        }
//            counter.awaitDoneWork();
        assertEquals(chunks, reader.getTotalChunks());
        assertEquals(byteCount, reader.getTotalRead());
        assertEquals(byteCount, total);
//            assertEquals(byteCount, counter.byteCount());
    }


//    @Test
////    @RepeatedTest(10)
//    public void countBytesPooledLincheck() {
//        Lincheck.runConcurrentTest(this::countBytesPooled);
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
