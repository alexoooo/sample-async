package io.github.alexoooo.sample.async.support;


import io.github.alexoooo.sample.async.AsyncState;
import io.github.alexoooo.sample.async.AsyncWorker;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;


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
        await(asyncWorker -> asyncWorker.state() == target, worker, duration);
    }


    public static void await(Predicate<AsyncWorker> condition, AsyncWorker worker) throws TimeoutException {
        await(condition, worker, stateTransitionDelay);
    }

    public static void await(
            Predicate<AsyncWorker> condition, AsyncWorker worker, Duration duration
    ) throws TimeoutException {
        Instant deadline = Instant.now().plus(duration);
        while (true) {
            boolean test = condition.test(worker);
            if (test) {
                return;
            }
            if (deadline.isBefore(Instant.now())) {
                throw new TimeoutException();
            }
        }
    }
}
