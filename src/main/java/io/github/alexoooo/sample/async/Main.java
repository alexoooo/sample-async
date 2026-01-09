package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.generic.io.FileChunk;
import io.github.alexoooo.sample.async.generic.io.FileLineCounter;
import io.github.alexoooo.sample.async.generic.io.FileReaderPooledProducer;
import io.github.alexoooo.sample.async.generic.io.FileReaderProducer;
import io.github.alexoooo.sample.async.producer.AsyncResult;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;


public class Main {
    //-----------------------------------------------------------------------------------------------------------------
    static void main(String[] args) throws Exception {
        IO.println("start: " + LocalTime.now());

//        Path path = Path.of("C:/~/data/measurements-100000.txt");
//        Path path = Path.of("C:/~/data/measurements-10000000.txt");
        Path path = Path.of("C:/~/data/measurements-1000000000.txt");

        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            heapRead(path);
//            pooledRead(path);
//            directRead(path);
//            pooledIterator(path);

            IO.println(i + " took: " + (System.currentTimeMillis() - start) + "ms");
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private static void heapRead(Path path) throws Exception {
        try (FileReaderProducer reader = FileReaderProducer.createStarted(
                path, 32 * 1024, 16);
             FileLineCounter counter = FileLineCounter.createStarted(16)
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

            IO.println("total: " + counter.byteCount() + " | " + counter.lineCount());
        }
    }


    private static void pooledRead(Path path) {
        try (FileReaderPooledProducer reader = FileReaderPooledProducer.createStarted(
                path, 32 * 1024, 16)
        ) {
            long total = 0;
            while (true) {
                AsyncResult<FileChunk> result = reader.poll();

                FileChunk value = result.value();
                if (value != null) {
                    total += value.length;
                    reader.release(value);
                }

                if (result.endReached()) {
                    break;
                }
            }

            IO.println("total: " + total);
        }
    }


    private static void pooledIterator(Path path) {
        try (FileReaderPooledProducer reader = FileReaderPooledProducer.createStarted(
                path, 32 * 1024, 16)
        ) {
            long total = 0;
            int lines = 1;
            while (reader.hasNext()) {
                FileChunk value = reader.next();
                for (int i = 0; i < value.length; i++) {
                    if (value.bytes[i] == '\n') {
                        lines++;
                    }
                }
                total += value.length;
                reader.release(value);
            }

            IO.println("total: " + total + " | " + lines);
        }
    }


    private static void directRead(Path path) throws Exception {
        try (InputStream reader = Files.newInputStream(path)) {
            byte[] buffer = new byte[32 * 1024];

            long total = 0;
            int lines = 1;
            while (true) {
                int read = reader.read(buffer);
                if (read == -1) {
                    break;
                }
                for (int i = 0; i < read; i++) {
                    if (buffer[i] == '\n') {
                        lines++;
                    }
                }
                total += read;
            }

            IO.println("total: " + total + " | " + lines);
        }
    }
}
