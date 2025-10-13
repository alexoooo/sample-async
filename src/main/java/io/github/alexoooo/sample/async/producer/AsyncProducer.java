package io.github.alexoooo.sample.async.producer;


import io.github.alexoooo.sample.async.AsyncWorker;

import java.util.function.Consumer;


public interface AsyncProducer<T>
        extends AsyncWorker, CloseableIterator<T>
{
    /**
     * @return how many results are ready to be polled, can be stale due to race condition
     * @throws RuntimeException to report asynchronous background failure
     */
    int available() throws RuntimeException;


    /**
     * @return try to read next available value, which might be the last one (or more might potentially be available)
     * @throws RuntimeException to report asynchronous background failure
     */
    AsyncResult<T> poll() throws RuntimeException;


    /**
     * @return true if more data might be available (the end of stream has not been reached yet)
     * @throws RuntimeException to report asynchronous background failure
     */
    boolean poll(Consumer<T> consumer) throws RuntimeException;
}
