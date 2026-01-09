package io.github.alexoooo.sample.async.producer.support;

import io.github.alexoooo.sample.async.producer.AbstractAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;


public class QueueProducer<T> extends AbstractAsyncProducer<T> {
    //-----------------------------------------------------------------------------------------------------------------
    public static <T> QueueProducer<T> createStarted() {
        QueueProducer<T> instance = new QueueProducer<>(1, Thread.ofPlatform().factory());
        instance.start();
        return instance;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final Queue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean ended = new AtomicBoolean();


    //-----------------------------------------------------------------------------------------------------------------
    public QueueProducer(int queueSizeLimit, ThreadFactory threadFactory) {
        super(queueSizeLimit, threadFactory);
    }


    //-----------------------------------------------------------------------------------------------------------------
    public void push(T value) {
        if (ended.get()) {
            throw new IllegalStateException();
        }
        queue.add(value);
    }


    public void end() {
        boolean set = ended.compareAndSet(false, true);
        if (!set) {
            throw new IllegalStateException();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected @Nullable T tryComputeNext() {
        T value = queue.poll();
        if (value == null && ended.get()) {
            return endReached();
        }
        return value;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected void init() {}

    @Override
    protected void closeAsyncImpl() {}

    @Override
    protected void closeImpl() {}
}
