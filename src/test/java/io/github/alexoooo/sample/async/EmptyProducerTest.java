package io.github.alexoooo.sample.async;

import io.github.alexoooo.sample.async.producer.AsyncResult;
import io.github.alexoooo.sample.async.support.AsyncTestUtils;
import io.github.alexoooo.sample.async.support.ControllableEmptyProducer;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;


public class EmptyProducerTest {
    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void emptyProducerLifecycleEndReached() throws InterruptedException, TimeoutException {
        ControllableEmptyProducer producer = new ControllableEmptyProducer();

        try {
            producer.poll();
            throw new AssertionFailedError();
        }
        catch (IllegalStateException expected) {}

        assertEquals(AsyncState.Created, producer.state());

        Thread asyncStart = new Thread(producer::start);
        asyncStart.start();
        AsyncTestUtils.awaitState(AsyncState.Starting, producer);

        producer.doInit();
        asyncStart.join();
        AsyncTestUtils.awaitState(AsyncState.Running, producer);

        AsyncResult<Void> pollWhileRunning = producer.poll();
        assertNull(pollWhileRunning.value());
        assertFalse(pollWhileRunning.endReached());

        producer.produceEndReached();
        AsyncTestUtils.await(AsyncWorker::isWorkFinished, producer);

        CountDownLatch closedRequested = new CountDownLatch(1);
        new Thread(() -> {
            closedRequested.countDown();
            producer.close();
        }).start();

        closedRequested.await();
        AsyncTestUtils.awaitState(AsyncState.Closing, producer);

        producer.doCloseAsync();
        producer.doClose();

        producer.awaitCloseAsync();
        AsyncTestUtils.awaitState(AsyncState.Terminal, producer);

        AsyncResult<Void> afterResult = producer.poll();
        assertNull(afterResult.value());
        assertTrue(afterResult.endReached());
    }


    @Test
    public void emptyProducerLifecycleClosed() throws InterruptedException, TimeoutException {
        ControllableEmptyProducer producer = new ControllableEmptyProducer();

        producer.doInit();
        producer.start();

        Thread asyncClose = new Thread(producer::close);
        asyncClose.start();
        producer.awaitCloseRequested();
        producer.produceNothing();
        AsyncTestUtils.awaitState(AsyncState.Closing, producer);

        producer.doCloseAsync();
        producer.doClose();
        asyncClose.join();
        AsyncTestUtils.awaitState(AsyncState.Terminal, producer);
    }
}
