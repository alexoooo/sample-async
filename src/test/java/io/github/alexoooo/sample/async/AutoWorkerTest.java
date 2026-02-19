package io.github.alexoooo.sample.async;

import io.github.alexoooo.sample.async.auto.AutoAsyncConsumer;
import io.github.alexoooo.sample.async.auto.AutoAsyncProducer;
import io.github.alexoooo.sample.async.auto.AutoPooledAsyncProducer;
import io.github.alexoooo.sample.async.consumer.AsyncConsumer;
import io.github.alexoooo.sample.async.generic.IteratorProducer;
import io.github.alexoooo.sample.async.generic.io.FileChunk;
import io.github.alexoooo.sample.async.generic.io.FileLineCounter;
import io.github.alexoooo.sample.async.generic.io.FileReaderPooledProducer;
import io.github.alexoooo.sample.async.producer.AsyncProducer;
import io.github.alexoooo.sample.async.producer.PooledAsyncProducer;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;


public class AutoWorkerTest
{
    //-----------------------------------------------------------------------------------------------------------------
    @SuppressWarnings("BusyWait")
    @Test
    public void detectedConsumerLeak() throws InterruptedException {
        AtomicBoolean leaked = new AtomicBoolean(false);
        abandonAutoConsumer(_ -> leaked.set(true));
        while (!leaked.get()) {
            System.gc();
            Thread.sleep(100);
            var _ = new int[10_000];
        }
    }

    private void abandonAutoConsumer(
            Consumer<AsyncConsumer<FileChunk>> leakCallback
    ) throws InterruptedException {
        FileLineCounter consumer = FileLineCounter.createStarted(1);
        Thread.sleep(100);
        new AutoAsyncConsumer<>(consumer, leakCallback);
    }


    @Test
    public void detectedConsumerClosed() {
        AtomicBoolean leaked = new AtomicBoolean(false);
        FileLineCounter delegate = FileLineCounter.createStarted(1);
        var auto = new AutoAsyncConsumer<>(delegate, _ -> leaked.set(true));
        auto.close();
        assertFalse(leaked.get());
    }


    //-----------------------------------------------------------------------------------------------------------------
    @SuppressWarnings("BusyWait")
    @Test
    public void detectedPooledProducerLeak() throws InterruptedException {
        AtomicBoolean leaked = new AtomicBoolean(false);
        abandonAutoPooledProducer(_ -> leaked.set(true));
        while (!leaked.get()) {
            System.gc();
            Thread.sleep(100);
            var _ = new int[10_000];
        }
    }

    private void abandonAutoPooledProducer(
            Consumer<PooledAsyncProducer<FileChunk>> leakCallback
    ) throws InterruptedException {
        Supplier<InputStream> bytes = LineCountTest.generateBytes(128);
        PooledAsyncProducer<FileChunk> pooledProducer = FileReaderPooledProducer.createStarted(
                bytes, 5, 5);
        Thread.sleep(100);
        new AutoPooledAsyncProducer<>(pooledProducer, leakCallback);
    }


    //-----------------------------------------------------------------------------------------------------------------
    @SuppressWarnings("BusyWait")
    @Test
    public void detectedProducerLeak() throws InterruptedException {
        AtomicBoolean leaked = new AtomicBoolean(false);
        abandonAutoProducer(_ -> leaked.set(true));
        while (!leaked.get()) {
            System.gc();
            Thread.sleep(100);
            var _ = new int[10_000];
        }
    }

    private void abandonAutoProducer(
            Consumer<AsyncProducer<String>> leakCallback
    ) throws InterruptedException {
        AsyncProducer<String> producer = IteratorProducer.createStarted(
                List.of("foo").iterator());
        Thread.sleep(100);
        new AutoAsyncProducer<>(producer, leakCallback);
    }
}
