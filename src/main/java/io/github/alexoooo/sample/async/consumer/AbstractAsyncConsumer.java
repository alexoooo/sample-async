package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;


public abstract class AbstractAsyncConsumer<T>
        extends AbstractAsyncWorker
        implements AsyncConsumer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    protected final int queueSizeLimit;

    private volatile @Nullable T pending;
    private final BlockingQueue<T> queue;
    private final Object workMonitor = new Object();
    private volatile boolean processingQueue;


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
    public void awaitDoneWork() throws RuntimeException {
        checkRunning();
        while (pending != null || !queue.isEmpty() || processingQueue) {
            sleepForPolling(workMonitor);
            checkRunning();
        }
    }


    @Override
    public final boolean offer(T item) {
        checkRunning();
        return queue.offer(item);
    }


    @Override
    public final int offer(List<T> items, int startingIndex) {
        checkRunning();

        int count = 0;
        for (int i = startingIndex, s = items.size(); i < s; i++) {
            T item = items.get(i);
            boolean added = queue.offer(item);
            if (added) {
                count++;
            }
            else {
                sleepForPolling(workMonitor);
                break;
            }
        }
        return count;
    }


    @Override
    public final void put(T item) {
        while (! closeRequested()) {
            throwExecutionExceptionIfRequired();

            boolean added = queue.offer(item);
            if (added) {
                return;
            }

            sleepForPolling(workMonitor);
        }

        if (closeRequested()) {
            throw new IllegalStateException("Close requested");
        }
    }


    @Override
    protected final boolean work() throws Exception {
        T localPending = pending;
        if (localPending != null) {
            boolean processed = tryProcessNext(localPending, false);
            if (processed) {
                pending = null;
            }
            else {
                sleepForBackoff();
                return true;
            }
        }
        else {
            T next = queue.peek();
            if (next == null) {
                sleepForBackoff();
                return true;
            }
            processingQueue = true;
            queue.remove();
            boolean processed = tryProcessNext(next, true);
            if (! processed) {
                pending = next;
                processingQueue = false;
                sleepForBackoff();
                return true;
            }
            processingQueue = false;
        }

        notifyItemProcessed();
        return true;
    }


    @Override
    protected final void closeImpl() throws Exception {
        try {
            if (! failed()) {
                T localPending = pending;
                if (localPending != null) {
                    processNext(localPending, true);
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
            sleepForBackoff();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected final void notifyItemProcessed() {
        synchronized (workMonitor) {
            workMonitor.notify();
        }
    }

    private void checkRunning() {
        if (! started) {
            throw new IllegalStateException("Not started");
        }
        if (closeRequested()) {
            throw new IllegalStateException("Close requested");
        }
        throwExecutionExceptionIfRequired();
    }


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * if item is not consumed, then the thread will sleep for a bit to avoid pinning
     * @return true if the item was consumed, otherwise the same item will be repeatedly re-submitted for processing
     */
    abstract protected boolean tryProcessNext(T item, boolean initialAttempt) throws Exception;

    abstract protected void doClose() throws Exception;
}
