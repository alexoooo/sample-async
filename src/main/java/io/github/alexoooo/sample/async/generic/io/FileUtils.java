package io.github.alexoooo.sample.async.generic.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;


public class FileUtils {
    private FileUtils() {}


    public static Supplier<InputStream> readerSupplier(Path path) {
        return () -> {
            try {
                return Files.newInputStream(path);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
