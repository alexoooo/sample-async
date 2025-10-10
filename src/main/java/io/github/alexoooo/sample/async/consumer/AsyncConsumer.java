package io.github.alexoooo.sample.async.consumer;


import io.github.alexoooo.sample.async.AsyncWorker;


public interface AsyncConsumer<T>
        extends AsyncWorker
{
    boolean offer(T item);

    void put(T item);
}
