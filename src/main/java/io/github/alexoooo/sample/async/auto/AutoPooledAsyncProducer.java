package io.github.alexoooo.sample.async.auto;

import io.github.alexoooo.sample.async.AsyncState;
import io.github.alexoooo.sample.async.producer.AsyncResult;
import io.github.alexoooo.sample.async.producer.PooledAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.lang.ref.Cleaner;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class AutoPooledAsyncProducer<T, D extends PooledAsyncProducer<T>>
        implements PooledAsyncProducer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final Cleaner cleaner = Cleaner.create();


    private record CleanupAction<T, D extends PooledAsyncProducer<T>>(
            D delegate,
            Consumer<D> leakCallback,
            AtomicBoolean closedExternally
    ) implements Runnable {
        @Override
        public void run() {
            if (!closedExternally.get()) {
                leakCallback.accept(delegate);
            }
            delegate.close();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final D delegate;
    private final CleanupAction<T, D> cleanupAction;
    private final Cleaner.Cleanable cleanable;


    //-----------------------------------------------------------------------------------------------------------------
    public AutoPooledAsyncProducer(D delegate, Consumer<D> leakCallback) {
        this.delegate = delegate;
        cleanupAction = new CleanupAction<>(delegate, leakCallback, new AtomicBoolean());
        cleanable = cleaner.register(this, cleanupAction);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override public void start() throws RuntimeException {
        delegate.start();
    }
    @Override public AsyncState state() {
        return delegate.state();
    }
    @Override public boolean isWorkFinished() {
        return delegate.isWorkFinished();
    }
    @Override public @Nullable Throwable failure() {
        return delegate.failure();
    }


    @Override public int available() throws RuntimeException {
        return delegate.available();
    }
    @Override public boolean isDone() {
        return delegate.isDone();
    }
    @Override public AsyncResult<T> peek() throws RuntimeException {
        return delegate.peek();
    }
    @Override public AsyncResult<T> poll() throws RuntimeException {
        return delegate.poll();
    }
    @Override public boolean poll(Collection<T> consumer) throws RuntimeException {
        return delegate.poll(consumer);
    }
    @Override public boolean hasNext() {
        return delegate.hasNext();
    }
    @Override public T next() {
        return delegate.next();
    }


    @Override public void release(T value) {
        delegate.release(value);
    }
    @Override public void releaseAll(List<T> values) {
        delegate.releaseAll(values);
    }


    @Override
    public void close() {
        cleanupAction.closedExternally.set(true);
        cleanable.clean();
    }
}
