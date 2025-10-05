package io.github.alexoooo.sample.async.io;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;
import org.jspecify.annotations.Nullable;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;


public class FileReaderWorker extends AbstractAsyncWorker<FileChunk> {
    //-----------------------------------------------------------------------------------------------------------------
    private final Path path;
    private final int chunkSize;

    private @Nullable InputStream inputStream;


    //-----------------------------------------------------------------------------------------------------------------
    public FileReaderWorker(Path path, int chunkSize, int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
        this.path = path;
        this.chunkSize = chunkSize;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() throws Exception {
        inputStream = Files.newInputStream(path);
    }


    @Override
    protected @Nullable FileChunk tryComputeNext() throws Exception {
        FileChunk chunk = new FileChunk(chunkSize);
        int read = Objects.requireNonNull(inputStream).read(chunk.bytes);
        if (read == -1) {
            return endReached();
        }
        chunk.length = read;
        return chunk;
    }


    @Override
    protected void closeImpl() throws Exception {
        Objects.requireNonNull(inputStream).close();
    }
}
