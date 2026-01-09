package io.github.alexoooo.sample.async.producer.support;


import io.github.alexoooo.sample.async.producer.AbstractAsyncProducer;
import io.github.alexoooo.sample.async.producer.AsyncProducer;
import io.github.alexoooo.sample.async.producer.AsyncResult;
import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ThreadFactory;


public class RepeatingProducer<T> extends AbstractAsyncProducer<T> {
    //-----------------------------------------------------------------------------------------------------------------
    public static <T> RepeatingProducer<T> createStarted(int count, AsyncProducer<T> source) {
        RepeatingProducer<T> instance = new RepeatingProducer<>(
                count, source, 1, Thread.ofPlatform().factory());
        instance.start();
        return instance;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final int count;
    private final AsyncProducer<T> source;
    private final Deque<T> pending = new ArrayDeque<>();
    private boolean ended = false;


    //-----------------------------------------------------------------------------------------------------------------
    public RepeatingProducer(
            int count,
            AsyncProducer<T> source,
            int queueSizeLimit,
            ThreadFactory threadFactory
    ) {
        super(queueSizeLimit, threadFactory);
        this.count = count;
        this.source = source;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected @Nullable T tryComputeNext() {
        T next = pending.poll();
        if (next != null) {
            return next;
        }
        if (ended) {
            return endReached();
        }

        AsyncResult<T> result = source.poll();

        T value = result.value();
        if (value != null) {
            for (int i = 0; i < count; i++) {
                pending.push(value);
            }
        }

        ended = result.endReached();
        skipBackoff();
        return null;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() {}

    @Override
    protected void closeAsyncImpl() {}

    @Override
    protected void closeImpl() {}
}
