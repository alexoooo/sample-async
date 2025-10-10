package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadFactory;


public abstract class AbstractAsyncConsumer<T>
        extends AbstractAsyncWorker
        implements AsyncConsumer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int queueFullSleepMillis = 25;


    //-----------------------------------------------------------------------------------------------------------------
    protected final int queueSize;

    private final Deque<T> deque = new ConcurrentLinkedDeque<>();
    private final Object workMonitor = new Object();


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractAsyncConsumer(int queueSize, ThreadFactory threadFactory) {
        super(threadFactory);
        this.queueSize = queueSize;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final boolean offer(T item) {
        if (closeRequested.get()) {
            throw new IllegalStateException("Close requested");
        }

        throwExecutionExceptionIfRequired();

        if (deque.size() >= queueSize) {
            return false;
        }

        deque.addLast(item);
        return true;
    }


    @Override
    public final void put(T item) {
        while (! closeRequested.get()) {
            throwExecutionExceptionIfRequired();

            if (deque.size() >= queueSize) {
                synchronized (workMonitor) {
                    try {
                        workMonitor.wait(queueFullSleepMillis);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                continue;
            }

            deque.addLast(item);
            return;
        }

        if (closeRequested.get()) {
            throw new IllegalStateException("Close requested");
        }
    }


    @Override
    protected final boolean work() throws Exception {
        T item = deque.pollFirst();
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
            while (! deque.isEmpty()) {
                processNext(deque.removeFirst());
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
