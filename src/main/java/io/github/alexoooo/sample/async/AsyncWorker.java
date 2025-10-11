package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;


/**
 * Thread-safe.
 * Automatically closed when work is done or failure is encountered, can be closed externally at any time.
 */
public interface AsyncWorker
        extends AutoCloseable
{
    void start();


    boolean closeAsync();

    @Override
    void close();


    AsyncState state();
    boolean workFinished();
    @Nullable Throwable failure();
}
