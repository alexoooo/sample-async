package io.github.alexoooo.sample.async;


import java.util.concurrent.ExecutionException;


public interface AsyncWorker<T>
        extends AutoCloseable
{
    void start() throws ExecutionException;

    AsyncResult<T> poll() throws ExecutionException;

    @Override
    void close() throws ExecutionException;
}
