package io.github.alexoooo.sample.async;


import java.util.concurrent.ExecutionException;


public interface PooledAsyncWorker<T> extends AsyncWorker<T> {
    void release(T value) throws ExecutionException;
}
