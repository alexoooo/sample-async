package io.github.alexoooo.sample.async;


import io.github.alexoooo.sample.async.generic.IteratorProducer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;


public class IteratorProducerTest {
    @Test
    public void emptyIterator() {
        Iterator<String> iterator = Collections.emptyIterator();
        try (IteratorProducer<String> producer = IteratorProducer.createStarted(iterator)) {
            assertFalse(producer.hasNext());
        }
    }


    @Test
    public void singleIterator() {
        Iterator<String> iterator = Collections.singletonList("foo").iterator();
        try (IteratorProducer<String> producer = IteratorProducer.createStarted(iterator)) {
            assertTrue(producer.hasNext());
            assertEquals("foo", producer.next());
            assertFalse(producer.hasNext());
        }
    }


    @Test
    public void manyIterator() {
        int size = 1_000_000;
        Iterator<Integer> iterator = IntStream.range(0, size).boxed().iterator();
        try (IteratorProducer<Integer> producer = IteratorProducer.createStarted(iterator, 16)) {
            List<Integer> buffer = new ArrayList<>();
            int outputCount = 0;
            while (producer.poll(buffer)) {
                outputCount += buffer.size();
                buffer.clear();
            }
            assertEquals(size, outputCount);
        }
    }
}
