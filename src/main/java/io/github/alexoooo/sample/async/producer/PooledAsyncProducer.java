package io.github.alexoooo.sample.async.producer;

import java.util.List;


public interface PooledAsyncProducer<T>
        extends AsyncProducer<T>
{
    void release(T value);
    void releaseAll(List<T> values);
}
