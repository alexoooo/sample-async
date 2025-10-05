package io.github.alexoooo.sample.async;


import java.util.concurrent.ExecutionException;


public interface AsyncWorker<T>
        extends CloseableIterator<T>
{
    void start() throws ExecutionException;

    AsyncResult<T> poll() throws ExecutionException;

    @Override
    void close() throws ExecutionException;
}
