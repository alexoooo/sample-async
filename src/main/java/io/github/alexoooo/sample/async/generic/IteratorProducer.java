package io.github.alexoooo.sample.async.generic;

import io.github.alexoooo.sample.async.producer.AbstractAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadFactory;


// TODO: handle closeAsync on iterator to avoid deadlock
public class IteratorProducer<T>
        extends AbstractAsyncProducer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    public static <T> IteratorProducer<T> createStarted(Iterator<T> iterator) {
        return createStarted(iterator, 1);
    }
    public static <T> IteratorProducer<T> createStarted(Iterator<T> iterator, int queueSizeLimit) {
        return createStarted(iterator, queueSizeLimit, Thread::new);
    }
    public static <T> IteratorProducer<T> createStarted(
            Iterator<T> iterator, int queueSizeLimit, ThreadFactory threadFactory) {
        IteratorProducer<T> instance = new IteratorProducer<>(iterator, queueSizeLimit, threadFactory);
        instance.start();
        return instance;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final Iterator<T> iterator;


    //-----------------------------------------------------------------------------------------------------------------
    public IteratorProducer(Iterator<T> iterator, int queueSizeLimit, ThreadFactory threadFactory) {
        super(queueSizeLimit, threadFactory);
        this.iterator = iterator;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected @Nullable T tryComputeNext() {
        if (!iterator.hasNext()) {
            return endReached();
        }
        return iterator.next();
    }


    @Override
    protected void doClose(List<T> remaining) throws Exception {
        if (iterator instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }
}
