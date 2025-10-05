package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.io.FileReaderWorker;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;


@SuppressWarnings("CallToPrintStackTrace")
public class Main {
    static void main(String[] args) {
//        Path path = Path.of("C:/~/data/measurements-10000000.txt");
        Path path = Path.of("C:/~/data/measurements-1000000000.txt");

        long start = System.currentTimeMillis();
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
        catch (ExecutionException e) {
            e.getCause().printStackTrace();
        }

        IO.println("took: " + (System.currentTimeMillis() - start));
    }
}
