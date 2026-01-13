package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.support.AsyncTestUtils;
import io.github.alexoooo.sample.async.support.ControllableFailingWorker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


public class FailingWorkerTest {
    @Test
    public void failOnInit() throws Exception {
        ControllableFailingWorker worker = new ControllableFailingWorker();

        worker.doInit(false);
        worker.doCloseAsync(true);
        worker.doClose(true);

        boolean startSuccessful;
        try {
            worker.start();
            startSuccessful = true;
        }
        catch (Exception e) {
            startSuccessful = false;
        }
        if (startSuccessful) {
            throw new IllegalStateException();
        }

        AsyncTestUtils.awaitState(AsyncState.Terminal, worker);
    }


    @ParameterizedTest
    @CsvSource(textBlock = """
        false
        true
        """)
    public void failOnFirstWork(boolean hasNext) throws Exception {
        ControllableFailingWorker worker = new ControllableFailingWorker();

        worker.doInit(true);
        worker.doCloseAsync(true);
        worker.doClose(true);

        worker.start();
        worker.doNextWork(hasNext, false);

        AsyncTestUtils.awaitState(AsyncState.Terminal, worker);
        assertNotNull(worker.failure());
    }


    @ParameterizedTest
    @CsvSource(textBlock = """
        false
        true
        """)
    public void failOnSecondWork(boolean hasNext) throws Exception {
        ControllableFailingWorker worker = new ControllableFailingWorker();

        worker.doInit(true);
        worker.doCloseAsync(true);
        worker.doClose(true);

        worker.doNextWork(true, true);
        worker.doNextWork(hasNext, false);

        worker.start();

        AsyncTestUtils.awaitState(AsyncState.Terminal, worker);
        assertNotNull(worker.failure());
    }


    @Test
    public void failOnCloseAsync() throws Exception {
        ControllableFailingWorker worker = new ControllableFailingWorker();

        worker.doInit(true);
        worker.doCloseAsync(false);
        worker.doClose(true);
        worker.doNextWork(false, true);

        worker.start();

        AsyncTestUtils.awaitState(AsyncState.Terminal, worker);
        assertNotNull(worker.failure());
    }


    @Test
    public void failOnClose() throws Exception {
        ControllableFailingWorker worker = new ControllableFailingWorker();

        worker.doInit(true);
        worker.doCloseAsync(true);
        worker.doClose(false);
        worker.doNextWork(false, true);

        worker.start();

        AsyncTestUtils.awaitState(AsyncState.Terminal, worker);
        assertNotNull(worker.failure());
    }


    @Test
    public void succeed() throws Exception {
        ControllableFailingWorker worker = new ControllableFailingWorker();

        worker.doInit(true);
        worker.doCloseAsync(true);
        worker.doClose(true);
        worker.doNextWork(false, true);

        worker.start();

        AsyncTestUtils.awaitState(AsyncState.Terminal, worker);
        assertNull(worker.failure());
    }
}
