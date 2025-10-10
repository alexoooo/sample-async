package io.github.alexoooo.sample.async;


public interface AsyncWorker
        extends AutoCloseable
{
    void start();


    @Override
    void close();
}
