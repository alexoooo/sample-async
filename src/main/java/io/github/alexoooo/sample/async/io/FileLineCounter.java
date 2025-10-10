package io.github.alexoooo.sample.async.io;

import io.github.alexoooo.sample.async.consumer.AbstractAsyncConsumer;

import java.util.concurrent.ThreadFactory;


public class FileLineCounter
        extends AbstractAsyncConsumer<FileChunk>
{
    //-----------------------------------------------------------------------------------------------------------------
    private long byteCount;
    private int lineCount;


    //-----------------------------------------------------------------------------------------------------------------
    public FileLineCounter(int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
    }


    //-----------------------------------------------------------------------------------------------------------------
    public long byteCount() {
        return byteCount;
    }

    public int lineCount() {
        return lineCount;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() {}


    @Override
    protected void processNext(FileChunk item) {
        if (lineCount == 0 && item.length > 0) {
            lineCount = 1;
        }

        byteCount += item.length;
        for (int i = 0; i < item.length; i++) {
            if (item.bytes[i] == '\n') {
                lineCount++;
            }
        }
    }

    @Override
    protected void doClose() {}
}
