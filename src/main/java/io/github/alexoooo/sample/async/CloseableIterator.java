package io.github.alexoooo.sample.async;

import java.util.Iterator;


public interface CloseableIterator<T>
        extends Iterator<T>, AutoCloseable
{
}
