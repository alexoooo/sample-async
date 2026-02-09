package io.github.alexoooo.sample.async.support;

import io.github.alexoooo.sample.async.producer.AbstractAsyncProducer;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


public class ControllableEmptyProducer
        extends AbstractAsyncProducer<Void>
{
    //-----------------------------------------------------------------------------------------------------------------
    private enum ResultType { Nothing, End }


    //-----------------------------------------------------------------------------------------------------------------
    private final CountDownLatch initLatch = new CountDownLatch(1);
//    private final CountDownLatch computeLatch = new CountDownLatch(1);
    private final CountDownLatch closeAsyncStartLatch = new CountDownLatch(1);
    private final CountDownLatch closeAsyncEndLatch = new CountDownLatch(1);
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private final BlockingQueue<ResultType> results = new LinkedBlockingQueue<>();


    //-----------------------------------------------------------------------------------------------------------------
    public ControllableEmptyProducer() {
        super(1, Thread.ofPlatform().factory());
    }


    //-----------------------------------------------------------------------------------------------------------------
    public void doInit() {
        initLatch.countDown();
    }

    public void doCloseAsync() {
        closeAsyncStartLatch.countDown();
    }

    public void awaitCloseAsync() throws InterruptedException {
        closeAsyncEndLatch.await();
    }

    public void doClose() {
        closeLatch.countDown();
    }

    public void produceNothing() {
        results.add(ResultType.Nothing);
    }

    public void produceEndReached() {
        results.add(ResultType.End);
    }

    @SuppressWarnings("BusyWait")
    public void awaitCloseRequested() {
        while (!closeRequested.get()) {
            try {
                Thread.sleep(1);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Override
    protected @Nullable Void tryComputeNext() throws InterruptedException {
        ResultType next = results.take();
        return next == ResultType.Nothing
                ? null
                : endReached();
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
    protected void doClose(List<Void> remaining) throws InterruptedException {
        closeLatch.await();
    }
}
