package io.github.alexoooo.sample.async;


import java.util.concurrent.ExecutionException;


public interface AsyncWorker
        extends AutoCloseable
{
    void start() throws ExecutionException;


    @Override
    void close() throws ExecutionException;
}
