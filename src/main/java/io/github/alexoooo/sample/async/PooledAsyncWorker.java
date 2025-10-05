package io.github.alexoooo.sample.async;


public interface PooledAsyncWorker<T> extends AsyncWorker<T> {
    void release(T value);
}
