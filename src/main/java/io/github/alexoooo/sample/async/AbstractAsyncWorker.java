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
    private static final int queueFullSleepMillis = 25;


    private record IteratorNext<T>(
            @Nullable T next,
            boolean checked
    ) {
        private static final IteratorNext<?> didNotCheck = new IteratorNext<>(null, false);
        private static final IteratorNext<?> endReached = new IteratorNext<>(null, true);
        @SuppressWarnings("unchecked")
        private static <T> IteratorNext<T> didNotCheck() {
            return (IteratorNext<T>) didNotCheck;
        }
        @SuppressWarnings("unchecked")
        private static <T> IteratorNext<T> endReached() {
            return (IteratorNext<T>) endReached;
        }
        private static <T> IteratorNext<T> of(T value) {
            return new IteratorNext<>(value, true);
        }
    }


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
    private final Object hasNextMonitor = new Object();
    private final Object eventLoopMonitor = new Object();

    private final AtomicReference<IteratorNext<T>> iteratorNext = new AtomicReference<>(IteratorNext.didNotCheck());


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
        return poll(false);
    }


    @SuppressWarnings("ConstantValue")
    private AsyncResult<T> poll(boolean forIterator) throws ExecutionException {
        if (! started) {
            throw new IllegalStateException("Not started");
        }

        throwExecutionExceptionIfRequired();

        if (! forIterator && ! iteratorNext.get().equals(IteratorNext.didNotCheck())) {
            throw new IllegalStateException("Iteration in progress");
        }

        if (deque.isEmpty()) {
            if (closeRan.getCount() == 0) {
                return AsyncResult.endReachedWithoutValue();
            }
            return AsyncResult.notReady();
        }

        T next = deque.pollFirst();
        if (next != null) {
            synchronized (eventLoopMonitor) {
                eventLoopMonitor.notify();
            }
            return AsyncResult.of(next);
        }
        else {
            return AsyncResult.notReady();
        }
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

        synchronized (hasNextMonitor) {
            hasNextMonitor.notify();
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
            sleepForPolling(eventLoopMonitor);
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
            synchronized (hasNextMonitor) {
                hasNextMonitor.notify();
            }
        }
    }


    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void sleepForPolling(Object monitor) {
        try {
            synchronized (monitor) {
                monitor.wait(queueFullSleepMillis);
            }
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
    @Override
    public boolean hasNext() {
        IteratorNext<T> current = iteratorNext.get();
        if (current.checked) {
            return current.next != null;
        }

        try {
            while (true) {
                AsyncResult<T> result = poll(true);
                if (result.value() == null && ! result.endReached()) {
                    sleepForPolling(hasNextMonitor);
                    continue;
                }

                boolean hasValue = result.value() != null;
                IteratorNext<T> check =
                        hasValue
                        ? IteratorNext.of(result.value())
                        : IteratorNext.endReached();

                boolean set = iteratorNext.compareAndSet(current, check);
                if (! set) {
                    throw new IllegalStateException("Concurrent modification");
                }

                return hasValue;
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }


    @Override
    public T next() {
        if (! hasNext()) {
            throw new IllegalStateException("Next not available");
        }

        IteratorNext<T> next = iteratorNext.getAndSet(IteratorNext.didNotCheck());
        if (next.next == null) {
            throw new IllegalStateException("Next expected");
        }
        return next.next;
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
