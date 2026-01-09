package io.github.alexoooo.sample.async.producer;


import io.github.alexoooo.sample.async.AsyncWorker;
import io.github.alexoooo.sample.async.producer.support.QueueProducer;
import io.github.alexoooo.sample.async.producer.support.RepeatingProducer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


public class ProducerChainTest {
    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void queueInOrder() {
        try (QueueProducer<Integer> queue = QueueProducer.createStarted()) {
            for (int i = 0; i < 10; i++) {
                queue.push(i);
                assertByPollWithPeek(i, queue);
            }
            queue.end();
            assertFalse(queue.hasNext());
        }
    }

    private void assertByPollWithPeek(int expected, QueueProducer<Integer> queue) {
        AsyncResult<Integer> peek;
        do {
            peek = queue.peek();
        } while (peek.value() == null);

        AsyncResult<Integer> poll = queue.poll();
        assertEquals(expected, poll.value());
        assertEquals(peek, poll);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void repeatTenTimes() {
        try (QueueProducer<Integer> queue = QueueProducer.createStarted();
             RepeatingProducer<Integer> repeating = RepeatingProducer.createStarted(10, queue)
        ) {
            queue.push(Integer.MIN_VALUE);
            queue.end();

            int count = countIdenticalUntilEndByIterator(Integer.MIN_VALUE, repeating);
            assertEquals(10, count);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private <T> int countIdenticalUntilEndByIterator(T expected, AsyncProducer<T> producer) {
        int count = 0;
        while (producer.hasNext()) {
            T next = producer.next();
            assertEquals(expected, next);
            count++;
        }
        return count;
    }


    //-----------------------------------------------------------------------------------------------------------------
    @Test
    public void repeatChain() {
        List<AsyncProducer<Integer>> chain = new ArrayList<>();
        try (QueueProducer<Integer> queue = QueueProducer.createStarted()) {
            AsyncProducer<Integer> previous = queue;
            for (int i = 0; i < 10; i++) {
                RepeatingProducer<Integer> duplicator = RepeatingProducer.createStarted(2, previous);
                chain.add(duplicator);
                previous = duplicator;
            }

            queue.push(-1);
            queue.end();

            int count = countIdenticalUntilEndByBuffer(-1, previous);
            assertEquals(1024, count);
        }
        finally {
            chain.forEach(AsyncWorker::close);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private <T> int countIdenticalUntilEndByBuffer(T expected, AsyncProducer<T> producer) {
        List<T> buffer = new ArrayList<>();
        int count = 0;
        while (true) {
            boolean hasNext = producer.poll(buffer);
            for (T item : buffer) {
                assertEquals(expected, item);
            }
            count += buffer.size();
            if (!hasNext) {
                break;
            }
            buffer.clear();
        }
        return count;
    }
}
