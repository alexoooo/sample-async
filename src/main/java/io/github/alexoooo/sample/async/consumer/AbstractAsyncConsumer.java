package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;


public abstract class AbstractAsyncConsumer<T>
        extends AbstractAsyncWorker
        implements AsyncConsumer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int queueFullSleepMillis = 25;


    //-----------------------------------------------------------------------------------------------------------------
    protected final int queueSizeLimit;

    private final BlockingQueue<T> queue;
    private final Object workMonitor = new Object();


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractAsyncConsumer(int queueSizeLimit, ThreadFactory threadFactory) {
        super(threadFactory);
        queue = new ArrayBlockingQueue<>(queueSizeLimit);
        this.queueSizeLimit = queueSizeLimit;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final int pending() {
        return queue.size();
    }


    @Override
    public final boolean offer(T item) {
        if (closeRequested()) {
            throw new IllegalStateException("Close requested");
        }

        throwExecutionExceptionIfRequired();

        return queue.offer(item);
    }


    @Override
    public final void put(T item) {
        while (! closeRequested()) {
            throwExecutionExceptionIfRequired();

            boolean added = queue.offer(item);
            if (added) {
                return;
            }

            synchronized (workMonitor) {
                try {
                    workMonitor.wait(queueFullSleepMillis);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (closeRequested()) {
            throw new IllegalStateException("Close requested");
        }
    }


    @Override
    protected final boolean work() throws Exception {
        T item = queue.poll();
        if (item == null) {
            return true;
        }

        processNext(item);

        synchronized (workMonitor) {
            workMonitor.notify();
        }

        return true;
    }


    @Override
    protected final void closeImpl() throws Exception {
        try {
            if (! failed()) {
                while (true) {
                    T item = queue.poll();
                    if (item == null) {
                        break;
                    }
                    processNext(item);
                }
            }
        }
        finally {
            doClose();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    abstract protected void processNext(T item) throws Exception;

    abstract protected void doClose() throws Exception;
}
