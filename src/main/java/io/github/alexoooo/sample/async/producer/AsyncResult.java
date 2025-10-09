package io.github.alexoooo.sample.async.producer;

import org.jspecify.annotations.Nullable;


public record AsyncResult<T>(
        @Nullable T value,
        boolean endReached
) {
    //-----------------------------------------------------------------------------------------------------------------
    private static final AsyncResult<?> notReady = new AsyncResult<>(null, false);
    private static final AsyncResult<?> endReachedWithoutValue = new AsyncResult<>(null, true);

    @SuppressWarnings("unchecked")
    public static <T> AsyncResult<T> notReady() {
        return (AsyncResult<T>) notReady;
    }

    @SuppressWarnings("unchecked")
    public static <T> AsyncResult<T> endReachedWithoutValue() {
        return (AsyncResult<T>) endReachedWithoutValue;
    }

    public static <T> AsyncResult<T> of(T value) {
        return new AsyncResult<>(value, false);
    }

    public static <T> AsyncResult<T> of(@Nullable T value, boolean endReached) {
        if (value != null) {
            return new AsyncResult<>(value, endReached);
        }
        return endReached ? endReachedWithoutValue() : notReady();
    }
}
