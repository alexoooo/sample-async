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
    private final Queue<T> pool;
    private @Nullable T pendingModel;


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractPooledAsyncProducer(int queueSize, ThreadFactory threadFactory) {
        super(queueSize, threadFactory);
        pool = new ArrayBlockingQueue<>(queueSize);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected final void init() throws Exception {
        doInit();

        for (int i = 0; i < queueSizeLimit; i++) {
            T item = Objects.requireNonNull(create());
            pool.add(item);
        }
    }


    protected abstract void doInit() throws Exception;


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final void release(T value) {
        throwExecutionExceptionIfRequired();

        boolean added = pool.add(value);
        if (! added) {
            throw new IllegalStateException("No space (" + queueSizeLimit + "): " + value);
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected final @Nullable T tryComputeNext() throws Exception {
        T value = pollNext();
        if (value == null) {
            return null;
        }

        boolean success = tryComputeNext(value);
        if (success) {
            pendingModel = null;
            return value;
        }
        return null;
    }


    private @Nullable T pollNext() {
        T existing = pendingModel;
        if (existing != null) {
            return existing;
        }

        T polled = pool.poll();
        if (polled != null) {
            clear(polled);
            pendingModel = polled;
        }
        return polled;
    }


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * @return new instance of pooled value (will be cleared before every use)
     */
    protected abstract T create();


    /**
     * Executes within the manager thread
     * @param value to be cleared (i.e. reset to original state) to be used again
     */
    protected abstract void clear(T value);


    /**
     * @param value pooled, will be continued with previous value after returning false
     * @return true if success/available/consumed, false if value was not consumed
     */
    protected abstract boolean tryComputeNext(T value) throws Exception;
}
