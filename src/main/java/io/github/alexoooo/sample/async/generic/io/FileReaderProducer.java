package io.github.alexoooo.sample.async.generic.io;


import io.github.alexoooo.sample.async.producer.AbstractAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;


public class FileReaderProducer extends AbstractAsyncProducer<FileChunk> {
    //-----------------------------------------------------------------------------------------------------------------
    public static FileReaderProducer createStarted(Path path, int chunkSize, int queueSize) {
        Supplier<InputStream> reader = FileUtils.readerSupplier(path);
        return createStarted(reader, chunkSize, queueSize);
    }

    public static FileReaderProducer createStarted(Supplier<InputStream> reader, int chunkSize, int queueSize) {
        FileReaderProducer instance = new FileReaderProducer(
                reader, chunkSize, queueSize, Thread.ofPlatform().factory());
        instance.start();
        return instance;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final Supplier<InputStream> reader;
    private final int chunkSize;

    private @Nullable InputStream inputStream;
    private long totalChunks = 0;
    private long totalRead = 0;


    //-----------------------------------------------------------------------------------------------------------------
    public FileReaderProducer(
            Supplier<InputStream> reader, int chunkSize, int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
        this.reader = reader;
        this.chunkSize = chunkSize;
    }


    //-----------------------------------------------------------------------------------------------------------------
    public long getTotalChunks() {
        return totalChunks;
    }
    public long getTotalRead() {
        return totalRead;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private InputStream inputStream() {
        return Objects.requireNonNull(inputStream);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() throws Exception {
//        inputStream = new BufferedInputStream(Files.newInputStream(path), 256 * 1024);
        inputStream = new BufferedInputStream(reader.get(), 256 * 1024);
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
        totalChunks++;
        totalRead += read;
        return chunk;
    }


    @Override
    protected void closeAsyncImpl() {}

    @Override
    protected void closeImpl() throws Exception {
        inputStream().close();
    }
}
