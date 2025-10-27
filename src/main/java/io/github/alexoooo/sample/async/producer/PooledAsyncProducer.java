package io.github.alexoooo.sample.async.producer;


public interface PooledAsyncProducer<T>
        extends AsyncProducer<T>
{
    void release(T value);
}
