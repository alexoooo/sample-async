package io.github.alexoooo.sample.async;


import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;


public interface AsyncWorker<T>
        extends CloseableIterator<T>
{
    void start() throws ExecutionException;

    AsyncResult<T> poll() throws ExecutionException;

    /**
     * @return true if more data might be available (the end of stream has not been reached yet)
     */
    boolean poll(Consumer<T> consumer) throws ExecutionException;

    @Override
    void close() throws ExecutionException;
}
