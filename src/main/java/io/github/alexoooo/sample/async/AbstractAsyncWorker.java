package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


public abstract class AbstractAsyncWorker
        implements AsyncWorker
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int sleepForPollingMillis = 1;
    private static final int sleepForBackoffNanos = 100_000;
    private static final int awaitMillisBeforeInterrupting = 5_000;


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
    private final AtomicReference<Boolean> skipBackoff = new AtomicReference<>(false);


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
        throw new RuntimeException(exception);
    }

    protected final void offerFirstException(Throwable exception) {
        if (closeRequested() && isInterruptedTransitively(exception)) {
            return;
        }
        firstException.compareAndSet(null, exception);
    }

    private boolean isInterruptedTransitively(Throwable exception) {
        Set<Throwable> chain = new LinkedHashSet<>();
        populateExceptionChain(exception, chain);
        for (Throwable t : chain) {
            if (t instanceof InterruptedException) {
                return true;
            }
        }
        return false;
    }

    private void populateExceptionChain(Throwable exception, Set<Throwable> chain) {
         boolean added = chain.add(exception);
         if (! added) {
             return;
         }
         if (exception.getCause() != null) {
             populateExceptionChain(exception.getCause(), chain);
         }
         for (Throwable suppressed : exception.getSuppressed()) {
             populateExceptionChain(suppressed, chain);
         }
    }


    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected final boolean failed() {
        return failure() != null;
    }

    protected final boolean closeRequested() {
        return closeRequested.get();
    }

    protected final boolean closed() {
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
            boolean closedBeforeTimeout = closed.await(awaitMillisBeforeInterrupting, TimeUnit.MILLISECONDS);

            if (! closedBeforeTimeout) {
                if (thread == null) {
                    throw new IllegalStateException("Thread missing");
                }
                thread.interrupt();
                closed.await();
            }

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
    abstract protected void init() throws Exception;


    /**
     * @return true if there is more work to do
     */
    abstract protected boolean work() throws Exception;


    /**
     * Should not rely on InterruptedException,
     *  it can hang (no way to trigger from the outside) or
     *  spuriously trigger (as part of closing logic which is aimed at init/work)
     * @throws Exception on logic or I/O failure
     */
    abstract protected void closeImpl() throws Exception;
}
