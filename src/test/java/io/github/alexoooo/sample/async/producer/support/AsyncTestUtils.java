package io.github.alexoooo.sample.async.producer.support;


import io.github.alexoooo.sample.async.AsyncState;
import io.github.alexoooo.sample.async.AsyncWorker;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;


public class AsyncTestUtils {
    //-----------------------------------------------------------------------------------------------------------------
    private static final Duration stateTransitionDelay = Duration.ofMillis(1_000);


    //-----------------------------------------------------------------------------------------------------------------
    private AsyncTestUtils() {}



    //-----------------------------------------------------------------------------------------------------------------
    public static void awaitState(AsyncState target, AsyncWorker worker) throws TimeoutException {
        awaitState(target, worker, stateTransitionDelay);
    }

    public static void awaitState(
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
