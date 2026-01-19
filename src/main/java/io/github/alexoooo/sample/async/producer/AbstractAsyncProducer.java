package io.github.alexoooo.sample.async.producer;


import io.github.alexoooo.sample.async.AbstractAsyncWorker;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractAsyncProducer<T>
        extends AbstractAsyncWorker
        implements AsyncProducer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
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
    private volatile boolean endReached = false;
    private volatile boolean computingNext = false;
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
    private void notifyEventLoop() {
        synchronized (eventLoopMonitor) {
            eventLoopMonitor.notify();
        }
    }


    private void notifyHasNext() {
        synchronized (hasNextMonitor) {
            hasNextMonitor.notify();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final int available() {
        throwExecutionExceptionIfRequired();
        return queue.size();
    }


    @Override
    public final boolean isDone() {
        if (failed()) {
            return true;
        }

        if (!endReached) {
            return false;
        }

        return !computingNext && queue.isEmpty();
    }


    @Override
    public final AsyncResult<T> poll() {
        return poll(false);
    }


    private AsyncResult<T> poll(boolean forIterator) {
        if (!started) {
            throw new IllegalStateException("Not started");
        }
        throwExecutionExceptionIfRequired();
        if (!forIterator && !iteratorNext.get().equals(IteratorNext.didNotCheck())) {
            throw new IllegalStateException("Iteration in progress");
        }

        T next = queue.poll();
        if (next == null) {
            if (endReached && !computingNext) {
                T nextAfterClosed = queue.poll();
                if (nextAfterClosed != null) {
                    return AsyncResult.of(nextAfterClosed, queue.isEmpty());
                }
                return AsyncResult.endReachedWithoutValue();
            }
            return AsyncResult.notReady();
        }

        notifyEventLoop();
        return AsyncResult.of(next);
    }


    @Override
    public final boolean poll(Collection<T> consumer) {
        if (!started) {
            throw new IllegalStateException("Not started");
        }
        throwExecutionExceptionIfRequired();
        if (!iteratorNext.get().equals(IteratorNext.didNotCheck())) {
            throw new IllegalStateException("Iteration in progress");
        }

        int drained = queue.drainTo(consumer);

        if (drained == 0) {
            boolean hasNext = !endReached || computingNext;
            if (!hasNext) {
                int drainedAfterClosed = queue.drainTo(consumer);
                if (drainedAfterClosed != 0) {
                    notifyEventLoop();
                }
            }
            return hasNext;
        }
        notifyEventLoop();
        return true;
    }


    @SuppressWarnings("ConstantValue")
    @Override
    public final AsyncResult<T> peek() throws RuntimeException {
        if (!started) {
            throw new IllegalStateException("Not started");
        }
        throwExecutionExceptionIfRequired();
        if (!iteratorNext.get().equals(IteratorNext.didNotCheck())) {
            throw new IllegalStateException("Iteration in progress");
        }

        T next = queue.peek();
        if (next == null) {
            if (endReached && !computingNext) {
                T nextAfterClosed = queue.peek();
                if (nextAfterClosed != null) {
                    return AsyncResult.of(nextAfterClosed);
                }
                return AsyncResult.endReachedWithoutValue();
            }
            return AsyncResult.notReady();
        }
        return AsyncResult.of(next);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected final boolean work() throws Exception {
        int size = queue.size();
        int remainingCapacity = queueSizeLimit - size;
        if (remainingCapacity == 0) {
            sleepForPolling(eventLoopMonitor);
            return true;
        }

        computingNext = true;
        int added = 0;
        for (int i = 0; i < remainingCapacity; i++) {
            T nextOrNull = tryComputeNext();
            if (nextOrNull != null) {
                boolean addedToQueue = queue.add(nextOrNull);
                if (!addedToQueue) {
                    throw new IllegalStateException();
                }

                added++;
                if (endReached) {
                    computingNext = false;
                    notifyHasNext();
                    return false;
                }
            }
            else {
                if (endReached) {
                    computingNext = false;
                    if (added > 0) {
                        notifyHasNext();
                    }
                    return false;
                }
                break;
            }
        }
        computingNext = false;

        if (added > 0) {
            notifyHasNext();
        }
        else {
            sleepForBackoff();
        }

        return true;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final boolean hasNext() {
        IteratorNext<T> current = iteratorNext.get();
        if (current.checked) {
            return current.next != null;
        }

        while (true) {
            AsyncResult<T> result = poll(true);
            if (result.value() == null && !result.endReached()) {
                sleepForPolling(hasNextMonitor);
                continue;
            }

            boolean hasValue = result.value() != null;
            IteratorNext<T> check =
                    hasValue
                    ? IteratorNext.of(result.value())
                    : IteratorNext.endReached();

            boolean set = iteratorNext.compareAndSet(current, check);
            if (!set) {
                throw new IllegalStateException("Concurrent modification");
            }

            return hasValue;
        }
    }


    @Override
    public final T next() {
        if (!hasNext()) {
            throw new IllegalStateException("Next not available");
        }

        IteratorNext<T> next = iteratorNext.getAndSet(IteratorNext.didNotCheck());
        if (next.next == null) {
            throw new IllegalStateException("Next expected");
        }
        return next.next;
    }


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * use to indicate end of data inside tryCompute,
     *  can be returned or called separately before the return in tryComputeNext
     * @return dummy value (null)
     */
    @SuppressWarnings("UnusedReturnValue")
    protected final @Nullable T endReached() {
        if (endReached) {
            throw new IllegalStateException("End already reached");
        }
        endReached = true;
        return null;
    }


    /**
     * Check if endReached was called.
     */
    @SuppressWarnings("unused")
    protected final boolean isEndReached() {
        return endReached;
    }

    /**
     * if item is not computed (null return), then the thread will sleep for a bit to avoid pinning
     * @return computed item, or null if not ready (call endReached() to indicate end of data)
     */
    abstract protected @Nullable T tryComputeNext() throws Exception;
}
