package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.io.FileReaderPooledWorker;
import io.github.alexoooo.sample.async.io.FileReaderWorker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;


public class Main {
    //-----------------------------------------------------------------------------------------------------------------
    static void main(String[] args) throws Exception {
//        Path path = Path.of("C:/~/data/measurements-10000000.txt");
        Path path = Path.of("C:/~/data/measurements-1000000000.txt");

        long start = System.currentTimeMillis();

//        heapRead(path);
        pooledRead(path);
//        directRead(path);

        IO.println("took: " + (System.currentTimeMillis() - start));
    }


    //-----------------------------------------------------------------------------------------------------------------
    private static void heapRead(Path path) throws ExecutionException {
        try (FileReaderWorker reader = new FileReaderWorker(
                path, 32 * 1024, 16, Thread.ofPlatform().factory())
        ) {
            reader.start();

            long total = 0;
            while (true) {
                AsyncResult<FileReaderWorker.Chunk> result = reader.poll();

                if (result.value() != null) {
                    total += result.value().length;
                }

                if (result.endReached()) {
                    break;
                }
            }

            IO.println("total: " + total);
        }
    }


    private static void pooledRead(Path path) throws ExecutionException {
        try (FileReaderPooledWorker reader = new FileReaderPooledWorker(
                path, 32 * 1024, 16, Thread.ofPlatform().factory())
        ) {
            reader.start();

            long total = 0;
            while (true) {
                AsyncResult<FileReaderWorker.Chunk> result = reader.poll();

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


    private static void directRead(Path path) throws Exception {
        try (InputStream reader = Files.newInputStream(path)) {
            byte[] buffer = new byte[32 * 1024];

            long total = 0;
            while (true) {
                int read = reader.read(buffer);
                if (read == -1) {
                    break;
                }
                total += read;
            }

            IO.println("total: " + total);
        }
    }
}
