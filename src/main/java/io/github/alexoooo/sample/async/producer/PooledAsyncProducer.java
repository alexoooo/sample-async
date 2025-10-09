package io.github.alexoooo.sample.async.producer;


import java.util.concurrent.ExecutionException;


public interface PooledAsyncProducer<T> extends AsyncProducer<T> {
    void release(T value) throws ExecutionException;
}
