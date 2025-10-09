package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AsyncWorker;

import java.util.concurrent.ExecutionException;


public interface AsyncConsumer<T>
        extends AsyncWorker
{
    boolean offer(T item) throws ExecutionException;

    void put(T item) throws ExecutionException;
}
