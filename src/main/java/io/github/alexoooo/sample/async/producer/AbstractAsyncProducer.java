package io.github.alexoooo.sample.async.producer;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


public abstract class AbstractAsyncProducer<T>
        extends AbstractAsyncWorker
        implements AsyncProducer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int queueFullSleepMillis = 25;


    private record IteratorNext<T>(
            @Nullable T next,
            boolean checked
    ) {
        private static final IteratorNext<?> didNotCheck = new IteratorNext<>(null, false);
        private static final IteratorNext<?> endReached = new IteratorNext<>(null, true);
        @SuppressWarnings("unchecked")
        private static <T> IteratorNext<T> didNotCheck() {
            return (IteratorNext<T>) didNotCheck;
        }
        @SuppressWarnings("unchecked")
        private static <T> IteratorNext<T> endReached() {
            return (IteratorNext<T>) endReached;
        }
        private static <T> IteratorNext<T> of(T value) {
            return new IteratorNext<>(value, true);
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected final int queueSizeLimit;
    private final BlockingQueue<T> queue;
    private final AtomicBoolean endReached = new AtomicBoolean();
    private final Object hasNextMonitor = new Object();
    private final Object eventLoopMonitor = new Object();

    private final AtomicReference<IteratorNext<T>> iteratorNext = new AtomicReference<>(IteratorNext.didNotCheck());


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * @param queueSizeLimit maximum queue size
     * @param threadFactory used to create worker thread
     */
    public AbstractAsyncProducer(int queueSizeLimit, ThreadFactory threadFactory) {
        super(threadFactory);
        queue = new ArrayBlockingQueue<>(queueSizeLimit);
        this.queueSizeLimit = queueSizeLimit;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final int available() {
        throwExecutionExceptionIfRequired();
        return queue.size();
    }


    @Override
    public final AsyncResult<T> poll() {
        return poll(false);
    }


    private AsyncResult<T> poll(boolean forIterator) {
        if (! started) {
            throw new IllegalStateException("Not started");
        }
        throwExecutionExceptionIfRequired();
        if (! forIterator && ! iteratorNext.get().equals(IteratorNext.didNotCheck())) {
            throw new IllegalStateException("Iteration in progress");
        }

        T next = queue.poll();

        if (next == null) {
            if (closed.getCount() == 0) {
                return AsyncResult.endReachedWithoutValue();
            }
            return AsyncResult.notReady();
        }

        synchronized (eventLoopMonitor) {
            eventLoopMonitor.notify();
        }
        return AsyncResult.of(next);
    }


    @Override
    public final boolean poll(Consumer<T> consumer) {
        if (! started) {
            throw new IllegalStateException("Not started");
        }
        throwExecutionExceptionIfRequired();
        if (! iteratorNext.get().equals(IteratorNext.didNotCheck())) {
            throw new IllegalStateException("Iteration in progress");
        }

        boolean empty = true;
        while (true) {
            T next = queue.poll();
            if (next == null) {
                break;
            }
            consumer.accept(next);
            empty = false;
        }

        if (empty) {
            return closed.getCount() != 0;
        }

        synchronized (eventLoopMonitor) {
            eventLoopMonitor.notify();
        }
        return true;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected boolean work() {
        if (queue.size() >= queueSizeLimit) {
            sleepForPolling(eventLoopMonitor);
            return true;
        }

        T nextOrNull;
        try {
            nextOrNull = tryComputeNext();
        }
        catch (Exception e) {
            firstException.compareAndSet(null, e);
            return false;
        }

        if (nextOrNull != null) {
            boolean added = queue.offer(nextOrNull);
            if (! added) {
                throw new IllegalStateException("Unable to add");
            }
            synchronized (hasNextMonitor) {
                hasNextMonitor.notify();
            }
        }

        return ! endReached.get();
    }


    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void sleepForPolling(Object monitor) {
        try {
            synchronized (monitor) {
                monitor.wait(queueFullSleepMillis);
            }
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public boolean hasNext() {
        IteratorNext<T> current = iteratorNext.get();
        if (current.checked) {
            return current.next != null;
        }

        while (true) {
            AsyncResult<T> result = poll(true);
            if (result.value() == null && ! result.endReached()) {
                sleepForPolling(hasNextMonitor);
                continue;
            }

            boolean hasValue = result.value() != null;
            IteratorNext<T> check =
                    hasValue
                    ? IteratorNext.of(result.value())
                    : IteratorNext.endReached();

            boolean set = iteratorNext.compareAndSet(current, check);
            if (! set) {
                throw new IllegalStateException("Concurrent modification");
            }

            return hasValue;
        }
    }


    @Override
    public T next() {
        if (! hasNext()) {
            throw new IllegalStateException("Next not available");
        }

        IteratorNext<T> next = iteratorNext.getAndSet(IteratorNext.didNotCheck());
        if (next.next == null) {
            throw new IllegalStateException("Next expected");
        }
        return next.next;
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected @Nullable T endReached() {
        boolean unique = endReached.compareAndSet(false, true);
        if (! unique) {
            throw new IllegalStateException("End already reached");
        }
        return null;
    }

    abstract protected @Nullable T tryComputeNext() throws Exception;
}
