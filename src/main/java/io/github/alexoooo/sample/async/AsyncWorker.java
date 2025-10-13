package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;


/**
 * Asynchronous (background) worker thread with associated lifecycle (see: AsyncState).
 * Automatically closed when work is done or failure is encountered, can be closed externally at any time.
 * Thread-safe.
 */
public interface AsyncWorker
        extends AutoCloseable
{
    //-----------------------------------------------------------------------------------------------------------------
    /**
     * Initializes state and starts background work thread.
     */
    void start();

    /**
     * Idempotent (can be called multiple times).
     * Doesn't throw anything, even if an exception was previously thrown in the background.
     * @return true if closing was newly requested
     */
    boolean closeAsync();

    /**
     * Attempts to close even if previously failed.
     * Idempotent (can be called multiple times), on subsequent calls background/closing exceptions will be thrown.
     * @throws RuntimeException if closing failed or to report exception previously thrown in background
     */
    @Override
    void close() throws RuntimeException;


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * Doesn't throw anything, even if an exception was previously thrown in the background.
     * Results could be stale due to race condition, except Terminal which is permanent.
     * @return processing stage, could be stale due to race condition.
     */
    AsyncState state();

    /**
     * Doesn't throw anything, even if an exception was previously thrown in the background.
     * Results could be stale due to race condition (e.g. work finished right after returning false).
     * @return true if all work was successfully completed (without exception/closing).
     */
    boolean workFinished();

    /**
     * Doesn't throw anything, even if an exception was previously thrown in the background.
     * Results could be stale due to race condition (e.g. failure happened right after returning null).
     * @return background error, or the first such error if multiple exceptions took place (null otherwise).
     */
    @Nullable Throwable failure();
}
