package io.github.alexoooo.sample.async.producer;


import io.github.alexoooo.sample.async.AsyncWorker;

import java.util.function.Consumer;


public interface AsyncProducer<T>
        extends AsyncWorker, CloseableIterator<T>
{
    /**
     * @return how many results are ready to be polled, can be stale due to race condition
     */
    int available();


    /**
     * @return try to read next available value, which might be the last one (or more might potentially be available)
     */
    AsyncResult<T> poll();


    /**
     * @return true if more data might be available (the end of stream has not been reached yet)
     */
    boolean poll(Consumer<T> consumer);
}
