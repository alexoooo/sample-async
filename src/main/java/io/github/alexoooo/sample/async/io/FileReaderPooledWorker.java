package io.github.alexoooo.sample.async.io;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;
import io.github.alexoooo.sample.async.AbstractPooledAsyncWorker;
import org.jspecify.annotations.Nullable;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;



public class FileReaderPooledWorker extends AbstractPooledAsyncWorker<FileReaderWorker.Chunk> {
    //-----------------------------------------------------------------------------------------------------------------
    private final Path path;
//    private final int chunkSize;

    private @Nullable InputStream inputStream;


    //-----------------------------------------------------------------------------------------------------------------
    public FileReaderPooledWorker(Path path, int chunkSize, int queueSize, ThreadFactory threadFactory) {
        super(() -> new FileReaderWorker.Chunk(chunkSize), queueSize, threadFactory);
        this.path = path;
//        this.chunkSize = chunkSize;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void doInit() throws Exception {
        inputStream = Files.newInputStream(path);
    }


    @Override
    protected boolean tryComputeNext(FileReaderWorker.Chunk chunk) throws Exception {
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
        if (inputStream != null) {
            inputStream.close();
        }
    }
}
