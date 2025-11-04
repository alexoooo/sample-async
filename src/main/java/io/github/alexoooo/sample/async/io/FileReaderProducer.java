package io.github.alexoooo.sample.async.io;


import io.github.alexoooo.sample.async.producer.AbstractAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;


public class FileReaderProducer extends AbstractAsyncProducer<FileChunk> {
    //-----------------------------------------------------------------------------------------------------------------
    private final Path path;
    private final int chunkSize;

    private @Nullable InputStream inputStream;


    //-----------------------------------------------------------------------------------------------------------------
    public FileReaderProducer(Path path, int chunkSize, int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
        this.path = path;
        this.chunkSize = chunkSize;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private InputStream inputStream() {
        return Objects.requireNonNull(inputStream);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() throws Exception {
        inputStream = new BufferedInputStream(Files.newInputStream(path), 256 * 1024);
    }


    @Override
    protected @Nullable FileChunk tryComputeNext() throws Exception {
        FileChunk chunk = new FileChunk(chunkSize);
        @SuppressWarnings("resource")
        int read = inputStream().read(chunk.bytes);
        if (read == -1) {
            return endReached();
        }
        chunk.length = read;
        return chunk;
    }


    @Override
    protected void closeAsyncImpl() {}

    @Override
    protected void closeImpl() throws Exception {
        inputStream().close();
    }
}
