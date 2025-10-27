package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


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
    protected final CountDownLatch closed = new CountDownLatch(1);
    private final CountDownLatch initiated = new CountDownLatch(1);
    private final AtomicReference<@Nullable Thread> threadHolder = new AtomicReference<>();
    protected final AtomicReference<@Nullable Throwable> firstException = new AtomicReference<>();


    //-----------------------------------------------------------------------------------------------------------------
    protected AbstractAsyncWorker(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected void throwExecutionExceptionIfRequired() {
        Throwable exception = failure();
        if (exception == null) {
            return;
        }
        throw new RuntimeException(exception);
    }

    protected void offerFirstException(Throwable exception) {
        firstException.compareAndSet(null, exception);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected boolean failed() {
        return failure() != null;
    }

    protected boolean closeRequested() {
        return closeRequested.get();
    }

    protected boolean closed() {
        return closed.getCount() == 0;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final void start() {
        boolean unique = startRequested.compareAndSet(false, true);
        if (! unique) {
            throw new IllegalStateException("Start already requested");
        }

        Thread thread = threadFactory.newThread(this::run);
        threadHolder.set(thread);
        thread.start();

        try {
            initiated.await();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        throwExecutionExceptionIfRequired();
    }


    //-----------------------------------------------------------------------------------------------------------------
    private void run() {
        state.set(AsyncState.Starting);
        boolean initSuccess = initInThread();

        if (initSuccess) {
            state.set(AsyncState.Running);
            loopInThread();
        }

        state.set(AsyncState.Closing);
        closeInThread();

        state.set(AsyncState.Terminal);
    }


    private boolean initInThread() {
        try {
            init();
            started = true;
            return true;
        }
        catch (Throwable e) {
            offerFirstException(e);
            return false;
        }
        finally {
            initiated.countDown();
        }
    }


    private void loopInThread() {
        try {
            while (! closeRequested() && ! failed()) {
                boolean hasMoreWork = work();
                if (! hasMoreWork) {
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
        try {
            closeImpl();
        }
        catch (Throwable e) {
            offerFirstException(e);
        }
        finally {
            closed.countDown();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final boolean closeAsync() {
        if (! started) {
            return false;
        }

        return closeRequested.compareAndSet(false, true);
    }


    @Override
    public final void close() {
        if (! started) {
            return;
        }

        closeAsync();

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

        throwExecutionExceptionIfRequired();
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final AsyncState state() {
        return state.get();
    }

    @Override
    public final boolean workFinished() {
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
        LockSupport.parkNanos(sleepForBackoffNanos);
    }


    //-----------------------------------------------------------------------------------------------------------------
    abstract protected void init() throws Exception;

    /**
     * @return true if there is more work to do
     */
    abstract protected boolean work() throws Exception;


    abstract protected void closeImpl() throws Exception;
}
