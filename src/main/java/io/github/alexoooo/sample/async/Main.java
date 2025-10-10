package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.io.FileChunk;
import io.github.alexoooo.sample.async.io.FileLineCounter;
import io.github.alexoooo.sample.async.io.FileReaderPooledProducer;
import io.github.alexoooo.sample.async.io.FileReaderProducer;
import io.github.alexoooo.sample.async.producer.AsyncResult;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;


public class Main {
    //-----------------------------------------------------------------------------------------------------------------
    static void main(String[] args) throws Exception {
//        Path path = Path.of("C:/~/data/measurements-10000000.txt");
        Path path = Path.of("C:/~/data/measurements-1000000000.txt");

        long start = System.currentTimeMillis();

        heapRead(path);
//        pooledRead(path);
//        directRead(path);
//        pooledIterator(path);

        IO.println("took: " + (System.currentTimeMillis() - start));
    }


    //-----------------------------------------------------------------------------------------------------------------
    private static void heapRead(Path path) {
        FileLineCounter counter = new FileLineCounter(
                16, Thread.ofPlatform().factory());

        try (FileReaderProducer reader = new FileReaderProducer(
                path, 32 * 1024, 16, Thread.ofPlatform().factory());
             counter
        ) {
            reader.start();
            counter.start();

            while (true) {
                AsyncResult<FileChunk> result = reader.poll();

                if (result.value() != null) {
                    counter.put(result.value());
                }

                if (result.endReached()) {
                    break;
                }
            }
        }

        IO.println("total: " + counter.byteCount() + " | " + counter.lineCount());
    }


    private static void pooledRead(Path path) {
        try (FileReaderPooledProducer reader = new FileReaderPooledProducer(
                path, 32 * 1024, 16, Thread.ofPlatform().factory())
        ) {
            reader.start();

            long total = 0;
            while (true) {
                AsyncResult<FileChunk> result = reader.poll();

                if (result.value() != null) {
                    total += result.value().length;
                    reader.release(result.value());
                }

                if (result.endReached()) {
                    break;
                }
            }

            IO.println("total: " + total);
        }
    }


    private static void pooledIterator(Path path) {
        try (FileReaderPooledProducer reader = new FileReaderPooledProducer(
                path, 32 * 1024, 16, Thread.ofPlatform().factory())
        ) {
            reader.start();

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
