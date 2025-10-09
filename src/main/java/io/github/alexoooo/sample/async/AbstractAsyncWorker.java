package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractAsyncWorker
        implements AsyncWorker
{
    //-----------------------------------------------------------------------------------------------------------------
    private final ThreadFactory threadFactory;

    private final AtomicBoolean startRequested = new AtomicBoolean();
    protected volatile boolean started = false;
    protected final AtomicBoolean closeRequested = new AtomicBoolean();
    protected final CountDownLatch closed = new CountDownLatch(1);
    private final CountDownLatch initiated = new CountDownLatch(1);
    private final AtomicReference<@Nullable Thread> threadHolder = new AtomicReference<>();
    protected final AtomicReference<@Nullable Exception> firstException = new AtomicReference<>();


    //-----------------------------------------------------------------------------------------------------------------
    protected AbstractAsyncWorker(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected void throwExecutionExceptionIfRequired() throws ExecutionException {
        Exception exception = firstException.get();
        if (exception == null) {
            return;
        }
        throw new ExecutionException(exception);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final void start() throws ExecutionException {
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
        started = true;
    }


    //-----------------------------------------------------------------------------------------------------------------
    private void run() {
        initInThread();
        loopInThread();
        closeInThread();
    }


    private void initInThread() {
        try {
            init();
        }
        catch (Exception e) {
            firstException.compareAndSet(null, e);
        }
        finally {
            initiated.countDown();
        }
    }


    private void loopInThread() {
        try {
            while (! closeRequested.get() &&
                    firstException.get() == null
            ) {
                boolean hasMoreWork = work();
                if (! hasMoreWork) {
                    break;
                }
            }
        }
        catch (Exception e) {
            firstException.compareAndSet(null, e);
        }
    }


    private void closeInThread() {
        try {
            closeImpl();
        }
        catch (Exception e) {
            firstException.compareAndSet(null, e);
        }
        finally {
            closed.countDown();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    public final void close() throws ExecutionException {
        if (! started) {
            return;
        }

        boolean unique = closeRequested.compareAndSet(false, true);
        if (! unique) {
            return;
        }

        Thread thread = threadHolder.getAndSet(null);
        if (thread == null) {
            throw new IllegalStateException();
        }

        try {
            closed.await();
            thread.join();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

//        synchronized (hasNextMonitor) {
//            hasNextMonitor.notify();
//        }

        throwExecutionExceptionIfRequired();
    }


    //-----------------------------------------------------------------------------------------------------------------
    abstract protected void init() throws Exception;

    /**
     * @return true if there is more work to do
     */
    abstract protected boolean work() throws Exception;


    abstract protected void closeImpl() throws Exception;
}
