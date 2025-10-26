package io.github.alexoooo.sample.async.io;

import io.github.alexoooo.sample.async.consumer.AbstractAsyncConsumer;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class FileLineCounter
        extends AbstractAsyncConsumer<FileChunk>
{
    //-----------------------------------------------------------------------------------------------------------------
    // NB: atomic because they are externally exposed via byteCount()/lineCount() methods
    private final AtomicLong byteCount = new AtomicLong();
    private final AtomicInteger lineCount = new AtomicInteger();


    //-----------------------------------------------------------------------------------------------------------------
    public FileLineCounter(int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
    }


    //-----------------------------------------------------------------------------------------------------------------
    public long byteCount() {
        return byteCount.get();
    }

    public int lineCount() {
        return lineCount.get();
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() {}


    @Override
    protected boolean tryProcessNext(FileChunk item) {
        if (lineCount.get() == 0 && item.length > 0) {
            lineCount.set(1);
        }

        byteCount.addAndGet(item.length);
        for (int i = 0; i < item.length; i++) {
            if (item.bytes[i] == '\n') {
                lineCount.incrementAndGet();
            }
        }

        return true;
    }

    @Override
    protected void doClose() {}
}
