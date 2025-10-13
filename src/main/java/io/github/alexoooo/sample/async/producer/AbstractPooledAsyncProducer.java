package io.github.alexoooo.sample.async.producer;


import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;


public abstract class AbstractPooledAsyncProducer<T>
        extends AbstractAsyncProducer<T>
        implements PooledAsyncProducer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private final Supplier<T> creator;
    private final Queue<T> pool;
    private @Nullable T pendingModel;


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractPooledAsyncProducer(Supplier<T> creator, int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
        this.creator = creator;
        pool = new ArrayBlockingQueue<T>(queueSize);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected final void init() throws Exception {
        doInit();

        for (int i = 0; i < queueSizeLimit; i++) {
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
            throw new IllegalStateException("No space (" + queueSizeLimit + "): " + value);
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
            pendingModel = null;
            return item;
        }
        return null;
    }


    private @Nullable T pollNext() {
        T existing = pendingModel;
        if (existing != null) {
            return existing;
        }

        T polled = pool.poll();
        pendingModel = polled;
        return polled;
    }


    /**
     * @param item pooled
     * @return true if success/available/consumed, false if item was not consumed
     */
    protected abstract boolean tryComputeNext(T item) throws Exception;
}
