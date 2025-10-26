package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;
import org.jspecify.annotations.Nullable;

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

    private @Nullable T pending;
    private final BlockingQueue<T> queue;
    private final Object workMonitor = new Object();


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractAsyncConsumer(int queueSizeLimit, ThreadFactory threadFactory) {
        super(threadFactory);
        queue = new ArrayBlockingQueue<>(queueSizeLimit);
        this.queueSizeLimit = queueSizeLimit;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private void awaitItemProcessedOrTimeout() {
        synchronized (workMonitor) {
            try {
                workMonitor.wait(queueFullSleepMillis);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected final void notifyItemProcessed() {
        synchronized (workMonitor) {
            workMonitor.notify();
        }
    }

    private void checkNotClosedOrFailed() {
        if (closeRequested()) {
            throw new IllegalStateException("Close requested");
        }
        throwExecutionExceptionIfRequired();
    }


    @Override
    public final int pending() {
        return queue.size();
    }


    @Override
    public void awaitZeroPending() throws RuntimeException {
        checkNotClosedOrFailed();
        while (pending() > 0) {
            awaitItemProcessedOrTimeout();
            checkNotClosedOrFailed();
        }
    }


    @Override
    public final boolean offer(T item) {
        checkNotClosedOrFailed();
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

            awaitItemProcessedOrTimeout();
        }

        if (closeRequested()) {
            throw new IllegalStateException("Close requested");
        }
    }


    @Override
    protected final boolean work() throws Exception {
        if (pending != null) {
            boolean processed = tryProcessNext(pending, false);
            if (processed) {
                pending = null;
            }
            else {
                return true;
            }
        }
        else {
            T next = queue.poll();
            if (next == null) {
                return true;
            }
            boolean processed = tryProcessNext(next, true);
            if (! processed) {
                pending = next;
                return true;
            }
        }

        notifyItemProcessed();
        return true;
    }


    @Override
    protected final void closeImpl() throws Exception {
        try {
            if (! failed()) {
                if (pending != null) {
                    processNext(pending, true);
                    pending = null;
                }

                while (true) {
                    T item = queue.poll();
                    if (item == null) {
                        break;
                    }
                    processNext(item, false);
                }
            }
        }
        finally {
            doClose();
        }
    }

    private void processNext(T item, boolean pending) throws Exception {
        boolean initialAttempt = ! pending;
        while (true) {
            boolean processed = tryProcessNext(item, initialAttempt);
            if (processed) {
                break;
            }
            initialAttempt = false;
            awaitItemProcessedOrTimeout();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * @return true if the item was consumed, otherwise the same item will be repeatedly re-submitted for processing
     */
    abstract protected boolean tryProcessNext(T item, boolean initialAttempt) throws Exception;

    abstract protected void doClose() throws Exception;
}
