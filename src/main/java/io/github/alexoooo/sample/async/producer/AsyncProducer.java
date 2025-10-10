package io.github.alexoooo.sample.async.producer;


import io.github.alexoooo.sample.async.AsyncWorker;

import java.util.function.Consumer;


public interface AsyncProducer<T>
        extends AsyncWorker, CloseableIterator<T>
{
    AsyncResult<T> poll();

    /**
     * @return true if more data might be available (the end of stream has not been reached yet)
     */
    boolean poll(Consumer<T> consumer);
}
