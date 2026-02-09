package io.github.alexoooo.sample.async.generic.io;


import io.github.alexoooo.sample.async.producer.AbstractPooledAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;


public class FileReaderPooledProducer
        extends AbstractPooledAsyncProducer<FileChunk>
{
    //-----------------------------------------------------------------------------------------------------------------
    public static FileReaderPooledProducer createStarted(Path path, int chunkSize, int queueSize) {
        Supplier<InputStream> reader = FileUtils.readerSupplier(path);
        return createStarted(reader, chunkSize, queueSize);
    }

    public static FileReaderPooledProducer createStarted(Supplier<InputStream> reader, int chunkSize, int queueSize) {
        FileReaderPooledProducer instance = new FileReaderPooledProducer(
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
    public FileReaderPooledProducer(
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
    @Override
    protected void doInit() {
        inputStream = reader.get();
    }


    @Override
    protected FileChunk create() {
        return new FileChunk(chunkSize);
    }


    @Override
    protected void clear(FileChunk value) {
        value.length = 0;
    }


    @Override
    protected boolean tryComputeNext(FileChunk chunk, boolean initialAttempt) throws Exception {
        if (!initialAttempt) {
            throw new UnsupportedOperationException("unexpected");
        }

        int read = Objects.requireNonNull(inputStream).read(chunk.bytes);
        if (read == -1) {
            chunk.length = 0;
            endReached();
            return false;
        }
        chunk.length = read;
        totalChunks++;
        totalRead += read;
        return chunk.length > 0;
    }


    @Override
    protected void doClose(List<FileChunk> remaining) throws Exception {
        Objects.requireNonNull(inputStream).close();
    }
}
