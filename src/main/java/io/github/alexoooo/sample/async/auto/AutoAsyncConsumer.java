package io.github.alexoooo.sample.async.auto;

import io.github.alexoooo.sample.async.AsyncState;
import io.github.alexoooo.sample.async.consumer.AsyncConsumer;
import org.jspecify.annotations.Nullable;

import java.lang.ref.Cleaner;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


public class AutoAsyncConsumer<T, D extends AsyncConsumer<T>>
        implements AsyncConsumer<T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final Cleaner cleaner = Cleaner.create();


    private record CleanupAction<T, D extends AsyncConsumer<T>>(
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
    public AutoAsyncConsumer(D delegate, Consumer<D> leakCallback) {
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


    @Override public int pending() throws RuntimeException {
        return delegate.pending();
    }
    @Override public void awaitDoneWork() throws RuntimeException {
        delegate.awaitDoneWork();
    }
    @Override public boolean offer(T item) throws RuntimeException {
        return delegate.offer(item);
    }
    @Override public int offer(List<T> items, int startingIndex) throws RuntimeException {
        return delegate.offer(items, startingIndex);
    }
    @Override public void put(T item) throws RuntimeException {
        delegate.put(item);
    }
    @Override public void putAll(List<T> items) throws RuntimeException {
        delegate.putAll(items);
    }


    @Override
    public void close() {
        cleanupAction.closedExternally.set(true);
        cleanable.clean();
    }
}
