package io.github.alexoooo.sample.async.support;

import io.github.alexoooo.sample.async.AbstractAsyncWorker;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;


public class ControllableFailingWorker
        extends AbstractAsyncWorker
{
    //-----------------------------------------------------------------------------------------------------------------


    //-----------------------------------------------------------------------------------------------------------------
    public ControllableFailingWorker() {
        super(Thread.ofPlatform().factory());
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final CompletableFuture<Boolean> initResult = new CompletableFuture<>();

    public void doInit(boolean success) {
        boolean completed = initResult.complete(success);
        if (!completed) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected void init() throws Exception {
        boolean success = initResult.get();
        if (!success) {
            throw new IllegalStateException();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private record WorkResult(boolean hasNext, boolean success) {}

    private final BlockingDeque<WorkResult> workResults = new LinkedBlockingDeque<>();
    private boolean workAllSuccess = true;

    public void doNextWork(boolean hasNext, boolean success) {
        if (!workAllSuccess) {
            throw new IllegalStateException();
        }
        workAllSuccess = success;

        boolean completed = workResults.add(new WorkResult(hasNext, success));
        if (!completed) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected boolean work() throws Exception {
        WorkResult result = workResults.takeFirst();
        if (!result.success()) {
            throw new IllegalStateException();
        }
        return result.hasNext();
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final CompletableFuture<Boolean> closeAsyncResult = new CompletableFuture<>();

    public void doCloseAsync(boolean success) {
        boolean completed = closeAsyncResult.complete(success);
        if (!completed) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected void closeAsyncImpl() throws Exception {
        boolean success = closeAsyncResult.get();
        if (!success) {
            throw new IllegalStateException();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private final CompletableFuture<Boolean> closeResult = new CompletableFuture<>();

    public void doClose(boolean success) {
        boolean completed = closeResult.complete(success);
        if (!completed) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected void closeImpl() throws Exception {
        boolean success = closeResult.get();
        if (!success) {
            throw new IllegalStateException();
        }
    }
}
