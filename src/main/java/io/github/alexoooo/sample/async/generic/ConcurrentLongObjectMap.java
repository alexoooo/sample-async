package io.github.alexoooo.sample.async.generic;


import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.locks.StampedLock;
import java.util.function.LongFunction;


public class ConcurrentLongObjectMap<T>
//    extends AbstractMap<Long, T>
//    implements ConcurrentMap<Long, T>
{
    //-----------------------------------------------------------------------------------------------------------------
    private static final int stripes = 1 << 4;


    //-----------------------------------------------------------------------------------------------------------------
    private final Long2ObjectMap<T>[] segments;
    private final StampedLock[] locks;


    //-----------------------------------------------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    public ConcurrentLongObjectMap() {
        segments = new Long2ObjectMap[stripes];
        locks = new StampedLock[stripes];
        for (int i = 0; i < stripes; i++) {
            segments[i] = new Long2ObjectOpenHashMap<>();
            locks[i] = new StampedLock();
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
    private int stripe(long key) {
        int hash = Long.hashCode(key);
        return (hash ^ (hash >>> 16)) & (stripes - 1);
    }


    public @Nullable T get(long key) {
        int stripe = stripe(key);
        StampedLock lock = locks[stripe];
        Long2ObjectMap<T> segment = segments[stripe];
        long stamp = lock.readLock();
        try {
            return segment.get(key);
        }
        finally {
            lock.unlockRead(stamp);
        }
    }


    public T put(long key, T value) {
        int stripe = stripe(key);
        StampedLock lock = locks[stripe];
        Long2ObjectMap<T> segment = segments[stripe];
        long stamp = lock.writeLock();
        try {
            return segment.put(key, value);
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }


    @SuppressWarnings("ConstantValue")
    public T computeIfAbsent(long key, LongFunction<T> mapping) {
        int stripe = stripe(key);
        StampedLock lock = locks[stripe];
        Long2ObjectMap<T> segment = segments[stripe];
        long stamp = lock.readLock();
        try {
            T value = segment.get(key);
            if (value != null) {
                return value;
            }

            long writeStamp = lock.tryConvertToWriteLock(stamp);
            if (writeStamp != 0) {
                stamp = writeStamp;
            }
            else {
                lock.unlockRead(stamp);
                stamp = 0;
                writeStamp = lock.writeLock();
                stamp = writeStamp;

                value = segment.get(key);
                if (value != null) {
                    return value;
                }
            }

            value = mapping.apply(key);
            segment.put(key, Objects.requireNonNull(value));
            return value;
        }
        finally {
            if (stamp != 0) {
                lock.unlock(stamp);
            }
        }
    }


    @SuppressWarnings("ConstantValue")
    public @Nullable T putIfAbsent(long key, T value) {
        int stripe = stripe(key);
        StampedLock lock = locks[stripe];
        Long2ObjectMap<T> segment = segments[stripe];
        long stamp = lock.readLock();
        try {
            T existing = segment.get(key);
            if (existing != null) {
                return existing;
            }

            long writeStamp = lock.tryConvertToWriteLock(stamp);
            if (writeStamp != 0) {
                stamp = writeStamp;
            }
            else {
                lock.unlockRead(stamp);
                stamp = 0;
                writeStamp = lock.writeLock();
                stamp = writeStamp;

                existing = segment.get(key);
                if (existing != null) {
                    return existing;
                }
            }

            segment.put(key, value);
            return null;
        }
        finally {
            if (stamp != 0) {
                lock.unlock(stamp);
            }
        }
    }


    public @Nullable T remove(long key) {
        int stripe = stripe(key);
        StampedLock lock = locks[stripe];
        Long2ObjectMap<T> segment = segments[stripe];
        long stamp = lock.writeLock();
        try {
            return segment.remove(key);
        }
        finally {
            lock.unlockWrite(stamp);
        }
    }


    public boolean containsKey(long key) {
        int stripe = stripe(key);
        StampedLock lock = locks[stripe];
        Long2ObjectMap<T> segment = segments[stripe];
        long stamp = lock.readLock();
        try {
            return segment.containsKey(key);
        }
        finally {
            lock.unlockRead(stamp);
        }
    }


//    @Override
    public int size() {
        int total = 0;
        long[] stamps = new long[stripes];

        try {
            for (int i = 0; i < stripes; i++) {
                stamps[i] = locks[i].readLock();
            }

            for (int i = 0; i < stripes; i++) {
                total += segments[i].size();
            }

            return total;
        }
        finally {
            for (int i = 0; i < stripes; i++) {
                if (stamps[i] != 0) {
                    locks[i].unlockRead(stamps[i]);
                }
            }
        }
    }


//    @Override
    public void clear() {
        long[] stamps = new long[stripes];

        try {
            for (int i = 0; i < stripes; i++) {
                stamps[i] = locks[i].writeLock();
            }

            for (int i = 0; i < stripes; i++) {
                segments[i].clear();
            }
        }
        finally {
            for (int i = 0; i < stripes; i++) {
                if (stamps[i] != 0) {
                    locks[i].unlockWrite(stamps[i]);
                }
            }
        }
    }


    //-----------------------------------------------------------------------------------------------------------------
//    @Override
//    public @Nullable T get(Object key) {
//        Long cast = (Long) key;
//    }
//
//
//    public T put(long key, T value) {
//    }
//
//    @Override
//    public Set<Entry<Long, T>> entrySet() {
//        return Set.of();
//    }
//
//    @Override
//    public T putIfAbsent(Long key, T value) {
//        return putIfAbsent(key.longValue(), value);
//    }
//
//    @Override
//    public boolean remove(Object key, Object value) {
//        return false;
//    }
//
//    @Override
//    public boolean replace(Long key, T oldValue, T newValue) {
//        return false;
//    }
//
//    @Override
//    public T replace(Long key, T value) {
//        return null;
//    }
}