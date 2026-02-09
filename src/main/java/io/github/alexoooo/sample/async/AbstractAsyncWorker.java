package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


/**
 * InterruptedException is not supported, stop by calling close(),
 *  and if underlying implementation needs to be interrupted then can do that in closeAsyncImpl()
 */
public abstract class AbstractAsyncWorker
        implements AsyncWorker
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int sleepForPollingMillis = 1;
    private static final int sleepForBackoffNanos = 100_000;


    //-----------------------------------------------------------------------------------------------------------------
    private final ThreadFactory threadFactory;

    private final AtomicBoolean startRequested = new AtomicBoolean();
    protected volatile boolean started = false;
    protected volatile boolean workFinished = false;
    protected final AtomicReference<AsyncState> state = new AtomicReference<>(AsyncState.Created);
    protected final AtomicBoolean closeRequested = new AtomicBoolean();
    protected final AtomicReference<@Nullable RuntimeException> closeRequest = new AtomicReference<>();
    protected final CountDownLatch closed = new CountDownLatch(1);
    private final CountDownLatch initiated = new CountDownLatch(1);
    private final AtomicReference<@Nullable Thread> threadHolder = new AtomicReference<>();
    protected final AtomicReference<@Nullable Throwable> initException = new AtomicReference<>();
    protected final AtomicReference<@Nullable Throwable> firstException = new AtomicReference<>();
    protected final AtomicBoolean exceptionThrown = new AtomicBoolean();
    private final AtomicBoolean skipBackoff = new AtomicBoolean();


    //-----------------------------------------------------------------------------------------------------------------
    protected AbstractAsyncWorker(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected final void throwExecutionExceptionIfRequired() {
        Throwable exception = failure();
        if (exception == null) {
            return;
        }
        exceptionThrown.set(true);
        throw new RuntimeException(exception);
    }

    protected final void offerFirstException(Throwable exception) {
        firstException.compareAndSet(null, exception);
    }


    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected final boolean failed() {
        return failure() != null;
    }

    protected final boolean closeRequested() {
        return closeRequested.get();
    }

    protected final @Nullable Throwable closeRequest() {
        return closeRequest.get();
    }

    protected final boolean closed() {
        return closed.getCount() == 0;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final void start() {
        boolean unique = startRequested.compareAndSet(false, true);
        if (!unique) {
            throw new IllegalStateException("Start already requested");
        }

        Thread thread = threadFactory.newThread(this::run);
        threadHolder.set(thread);
        thread.start();

        try {
            initiated.await();
        }
        catch (InterruptedException e) {
            offerFirstException(e);
            throw new IllegalStateException(e);
        }

        Throwable exception = initException.get();
        if (exception != null) {
            offerFirstException(exception);
            throw new RuntimeException(exception);
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private void run() {
        state.set(AsyncState.Starting);
        boolean initSuccess = initInThread();

        if (initSuccess) {
            state.set(AsyncState.Running);
            loopInThread();
        }

        closeInThread();
    }


    private boolean initInThread() {
        try {
            init();
            started = true;
            return true;
        }
        catch (Throwable e) {
            initException.compareAndSet(null, e);
            offerFirstException(e);
            return false;
        }
        finally {
            initiated.countDown();
        }
    }


    private void loopInThread() {
        try {
            while (!closeRequested() && !failed()) {
                boolean hasMoreWork = work();
                if (!hasMoreWork) {
                    workFinished = true;
                    break;
                }
            }
        }
        catch (Throwable e) {
            offerFirstException(e);
        }
    }


    private void closeInThread() {
        awaitFailedOrCloseRequested();

        state.set(AsyncState.Closing);
        try {
            closeAsync();
            closeImpl();
        }
        catch (Throwable e) {
            offerFirstException(e);
        }
        finally {
            closed.countDown();
            state.set(AsyncState.Terminal);
        }
    }

    @SuppressWarnings("BusyWait")
    private void awaitFailedOrCloseRequested() {
        while (!(failed() || closeRequested())) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                offerFirstException(e);
            }
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private boolean closeAsync() {
        if (!started) {
            return false;
        }

        boolean newRequest = closeRequested.compareAndSet(false, true);
        if (newRequest) {
            closeRequest.set(new RuntimeException());
            try {
                closeAsyncImpl();
            }
            catch (Throwable t) {
                offerFirstException(t);
            }
        }
        return newRequest;
    }


    @Override
    public final void close() {
        if (!started) {
            return;
        }

        boolean newlyClosing = closeAsync();
        if (!newlyClosing) {
            return;
        }

        Thread thread = threadHolder.getAndSet(null);

        try {
            closed.await();
            if (thread != null) {
                thread.join();
            }
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        if (!exceptionThrown.get()) {
            throwExecutionExceptionIfRequired();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final AsyncState state() {
        return state.get();
    }

    @Override
    public final boolean isWorkFinished() {
        return workFinished;
    }

    @Override
    public final @Nullable Throwable failure() {
        return firstException.get();
    }


    //-----------------------------------------------------------------------------------------------------------------
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected final void sleepForPolling(Object monitor) {
        try {
            synchronized (monitor) {
                monitor.wait(sleepForPollingMillis);
            }
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }


    protected final void sleepForBackoff() {
        boolean changed = skipBackoff.compareAndSet(true, false);
        if (changed) {
            return;
        }
        LockSupport.parkNanos(sleepForBackoffNanos);
    }


    @SuppressWarnings("unused")
    protected final void skipBackoff() {
        skipBackoff.set(true);
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected void init() throws Exception {
        // optionally implemented by subclass
    }


    /**
     * @return true if there is more work to do
     */
    abstract protected boolean work() throws Exception;


    /**
     * If closing is done in stages, initiate the first asynchronous closing sequence (executes on an unknown thread,
     *      to be followed by a synchronous close method invocation).
     * If there is no concept of closing stages then this method doesn't need to be implemented,
     *      if implemented it will be automatically invoked by the close method.
     * One example of where this is useful is if the main thread is blocked and needs to be interrupted to close,
     *     then the interruption can be handled via closeAsync.
     * Idempotent (can be called multiple times).
     * Doesn't throw anything, even if an exception was previously thrown in the background.
     */
    protected void closeAsyncImpl() throws Exception {
        // optionally implemented by subclass
    }


    /**
     * @throws Exception on logic or I/O failure
     */
    protected void closeImpl() throws Exception {
        // optionally implemented by subclass
    }
}
