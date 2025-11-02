package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AsyncWorker;

import java.util.List;
import java.util.Queue;


/**
 * Closing will attempt to process any pending items (unless an exception was previously thrown).
 */
public interface AsyncConsumer<T>
        extends AsyncWorker
{
    /**
     * @return how many items are waiting to be processed, can be stale due to race condition
     * @throws RuntimeException to report asynchronous background failure
     */
    int pending() throws RuntimeException;


    /**
     * @throws RuntimeException to report asynchronous background failure, or close request while waiting
     */
    void awaitZeroPending() throws RuntimeException;


    /**
     * @param item attempt to add to processing queue
     * @return true if item was accepted (i.e. there was space in the processing queue)
     * @throws RuntimeException to report asynchronous background failure
     */
    boolean offer(T item) throws RuntimeException;


    /**
     * If there's not enough capacity to accept all the items, then a short pause is introduced
     * @param items attempt to add all to processing queue
     * @return number of items that were accepted (i.e. there was space in the processing queue)
     * @throws RuntimeException to report asynchronous background failure
     */
    @SuppressWarnings("UnusedReturnValue")
    int offer(List<T> items, int startingIndex) throws RuntimeException;


    /**
     * @param item that is synchronously added to the processing queue (blocking until success)
     * @throws RuntimeException to report asynchronous background failure, or close request while waiting to add
     */
    void put(T item) throws RuntimeException;
}
