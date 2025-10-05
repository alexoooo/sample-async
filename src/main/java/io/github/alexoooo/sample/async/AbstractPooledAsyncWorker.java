package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;


public abstract class AbstractPooledAsyncWorker<T>
        extends AbstractAsyncWorker<T>
        implements PooledAsyncWorker<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private final Supplier<T> creator;
    private final Queue<T> pool;
    private @Nullable T next;


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractPooledAsyncWorker(Supplier<T> creator, int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
        this.creator = creator;
        pool = new ArrayBlockingQueue<T>(queueSize);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected final void init() throws Exception {
        doInit();

        for (int i = 0; i < queueSize; i++) {
            T item = Objects.requireNonNull(creator.get());
            pool.add(item);
        }
    }


    protected abstract void doInit() throws Exception;


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public void release(T value) {
        boolean added = pool.add(value);
        if (! added) {
            throw new IllegalStateException("No space (" + queueSize + "): " + value);
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected final @Nullable T tryComputeNext() throws Exception {
        T item = pollNext();
        if (item == null) {
            return null;
        }

        boolean success = tryComputeNext(item);
        if (success) {
            next = null;
            return item;
        }
        return null;
    }


    private @Nullable T pollNext() {
        T existing = next;
        if (existing != null) {
            return existing;
        }

        T polled = pool.poll();
        next = polled;
        return polled;
    }


    /**
     * @param item pooled
     * @return true if success/available/consumed, false if item was not consumed
     */
    protected abstract boolean tryComputeNext(T item) throws Exception;
}
