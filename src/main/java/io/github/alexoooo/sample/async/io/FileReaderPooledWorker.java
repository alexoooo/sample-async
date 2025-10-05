package io.github.alexoooo.sample.async.io;


import io.github.alexoooo.sample.async.AbstractPooledAsyncWorker;
import org.jspecify.annotations.Nullable;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;


public class FileReaderPooledWorker extends AbstractPooledAsyncWorker<FileChunk> {
    //-----------------------------------------------------------------------------------------------------------------
    private final Path path;

    private @Nullable InputStream inputStream;


    //-----------------------------------------------------------------------------------------------------------------
    public FileReaderPooledWorker(Path path, int chunkSize, int queueSize, ThreadFactory threadFactory) {
        super(() -> new FileChunk(chunkSize), queueSize, threadFactory);
        this.path = path;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void doInit() throws Exception {
        inputStream = Files.newInputStream(path);
    }


    @Override
    protected boolean tryComputeNext(FileChunk chunk) throws Exception {
        int read = Objects.requireNonNull(inputStream).read(chunk.bytes);
        if (read == -1) {
            endReached();
            return false;
        }
        chunk.length = read;
        return chunk.length > 0;
    }


    @Override
    protected void closeImpl() throws Exception {
        Objects.requireNonNull(inputStream).close();
    }
}
