package io.github.alexoooo.sample.async;


import org.jspecify.annotations.Nullable;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractAsyncWorker<T> implements AsyncWorker<T> {
    //-----------------------------------------------------------------------------------------------------------------
    private static final int queueFullSleepMillis = 10;


    //-----------------------------------------------------------------------------------------------------------------
    protected final int queueSize;
    private final ThreadFactory threadFactory;

    private final AtomicBoolean startRequested = new AtomicBoolean();
    private volatile boolean started = false;
    private final AtomicBoolean closeRequested = new AtomicBoolean();
    private final CountDownLatch closeRan = new CountDownLatch(1);
    private final AtomicBoolean endReached = new AtomicBoolean();
    private final CountDownLatch initRan = new CountDownLatch(1);
    private final AtomicReference<@Nullable Thread> threadHolder = new AtomicReference<>();
    private final AtomicReference<@Nullable Exception> firstException = new AtomicReference<>();

    private final Deque<T> deque = new ConcurrentLinkedDeque<>();


    //-----------------------------------------------------------------------------------------------------------------
    public AbstractAsyncWorker(int queueSize, ThreadFactory threadFactory) {
        this.queueSize = queueSize;
        this.threadFactory = threadFactory;
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
            initRan.await();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        throwExecutionExceptionIfRequired();
        started = true;
    }


    @Override
    public final AsyncResult<T> poll() throws ExecutionException {
        if (! started) {
            throw new IllegalStateException("Not started");
        }

        throwExecutionExceptionIfRequired();

        if (deque.isEmpty()) {
            if (closeRan.getCount() == 0) {
                return AsyncResult.endReachedWithoutValue();
            }
            return AsyncResult.notReady();
        }

        T next = deque.pollFirst();
        return AsyncResult.ofPossiblyReady(next);
    }


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
            closeRan.await();
            thread.join();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        throwExecutionExceptionIfRequired();
    }


    protected void throwExecutionExceptionIfRequired() throws ExecutionException {
        Exception exception = firstException.get();
        if (exception == null) {
            return;
        }
        throw new ExecutionException(exception);
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
            initRan.countDown();
        }
    }


    private void loopInThread() {
        try {
            while (! closeRequested.get() &&
                    ! endReached.get() &&
                    firstException.get() == null
            ) {
                workInThread();
            }
        }
        catch (Exception e) {
            firstException.compareAndSet(null, e);
        }
    }


    private void workInThread() {
        if (deque.size() >= queueSize) {
            sleepInThread();
            return;
        }

        T nextOrNull;
        try {
            nextOrNull = tryComputeNext();
        }
        catch (Exception e) {
            firstException.compareAndSet(null, e);
            return;
        }

        if (nextOrNull != null) {
            deque.addLast(nextOrNull);
        }
    }


    private void sleepInThread() {
        try {
            Thread.sleep(queueFullSleepMillis);
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(e);
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
            closeRan.countDown();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    protected @Nullable T endReached() {
        boolean unique = endReached.compareAndSet(false, true);
        if (! unique) {
            throw new IllegalStateException("End already reached");
        }
        return null;
    }


    abstract protected void init() throws Exception;

    abstract protected @Nullable T tryComputeNext() throws Exception;

    abstract protected void closeImpl() throws Exception;
}
