package io.github.alexoooo.sample.async.producer;

import io.github.alexoooo.sample.async.AsyncState;
import io.github.alexoooo.sample.async.AsyncWorker;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AssertionFailureBuilder;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;


public class AsyncProducerTest {
    //-----------------------------------------------------------------------------------------------------------------
    private static final Duration stateTransitionDelay = Duration.ofMillis(1_000);


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void emptyProducerLifecycle() throws InterruptedException, TimeoutException {
        CountDownLatch initLatch = new CountDownLatch(1);
        CountDownLatch computeLatch = new CountDownLatch(1);
        CountDownLatch closeAsyncStartLatch = new CountDownLatch(1);
        CountDownLatch closeAsyncEndLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AsyncProducer<Void> emptyProducer = new AbstractAsyncProducer<>(
                1, Thread.ofPlatform().factory()) {
            @Override
            protected @Nullable Void tryComputeNext() throws InterruptedException {
                computeLatch.await();
                return endReached();
            }

            @Override
            protected void init() throws InterruptedException {
                initLatch.await();
            }

            @Override
            protected void closeAsyncImpl() throws InterruptedException {
                closeAsyncStartLatch.await();
                closeAsyncEndLatch.countDown();
            }

            @Override
            protected void closeImpl() throws InterruptedException {
                closeLatch.await();
            }
        };

        try {
            emptyProducer.poll();
            throw new AssertionFailedError();
        }
        catch (IllegalStateException expected) {}

        assertEquals(AsyncState.Created, emptyProducer.state());

        Thread asyncStarter = new Thread(emptyProducer::start);
        asyncStarter.start();
        awaitState(AsyncState.Starting, emptyProducer);

        initLatch.countDown();
        asyncStarter.join();
        awaitState(AsyncState.Running, emptyProducer);

        AsyncResult<Void> pollWhileRunning = emptyProducer.poll();
        assertNull(pollWhileRunning.value());
        assertFalse(pollWhileRunning.endReached());

        computeLatch.countDown();
        awaitState(AsyncState.Closing, emptyProducer);

//        emptyProducer
//        Thread asyncCloser = new Thread(emptyProducer::close);
//        asyncCloser.start();
//        awaitState(AsyncState.Closing, emptyProducer);

        closeAsyncStartLatch.countDown();
        closeAsyncEndLatch.await();
        assertEquals(AsyncState.Closing, emptyProducer.state());

        closeLatch.countDown();
//        asyncCloser.join();
        awaitState(AsyncState.Terminal, emptyProducer);

        AsyncResult<Void> pollAfterClose = emptyProducer.poll();
        assertNull(pollAfterClose.value());
        assertTrue(pollAfterClose.endReached());
    }


    //-----------------------------------------------------------------------------------------------------------------
    private void awaitState(AsyncState target, AsyncWorker worker) throws TimeoutException {
        awaitState(target, worker, stateTransitionDelay);
    }

    private void awaitState(
            AsyncState target, AsyncWorker worker, Duration duration
    ) throws TimeoutException {
        Instant deadline = Instant.now().plus(duration);
        while (true) {
            AsyncState state = worker.state();
            if (state.equals(target)) {
                return;
            }
            if (deadline.isBefore(Instant.now())) {
                throw new TimeoutException();
            }
        }
    }
}
