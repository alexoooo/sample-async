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
     * @throws IllegalStateException if start already requested
     * @throws RuntimeException to report exception thrown during initiation
     * @throws IllegalStateException if interrupted before initiation completed
     */
    void start() throws RuntimeException;


    /**
     * If closing is done in stages, initiate the first asynchronous closing sequence
     *      (to be followed by the synchronous close)
     * Idempotent (can be called multiple times).
     * Doesn't throw anything, even if an exception was previously thrown in the background.
     * @return true if closing was newly requested
     */
    @SuppressWarnings("UnusedReturnValue")
    boolean closeAsync();


    /**
     * Attempts to close (even if previously failed).
     * Idempotent (can be called multiple times), on subsequent calls background/closing exceptions will be thrown.
     * Invoked automatically when all work is done.
     * @throws RuntimeException if closing failed or to report exception previously thrown in background
     */
    @Override
    void close() throws RuntimeException;


    //-----------------------------------------------------------------------------------------------------------------
    /**
     * Asynchronous processing state, note that it doesn't reflect the state of business logic
     *      (e.g. background thread can be Terminal, but AsyncProducer can still have available).
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
