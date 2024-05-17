//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

/**
 * An immutable cache that is designed to be fast when accessed concurrently with read-heavy workloads.
 * Values are populated from a function when they are not found in the cache.
 * All values must be non-null.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
class ImmutableReadHeavyConcurrentCache<K, V> {

    private final Function<K, V> valueComputer;
    private final StampedLock lock = new StampedLock();
    private final Map<K, V> cache = new HashMap<>();

    /**
     * Creates a new cache.
     *
     * @param valueComputer computes the value for a key.
     */
    public ImmutableReadHeavyConcurrentCache(final Function<K, V> valueComputer) {
        this.valueComputer = valueComputer;
    }

    /**
     * Clears the cache.
     */
    synchronized void clear() {
        long stamp = lock.writeLock();
        try {
            cache.clear();
        } finally {
            lock.unlock(stamp);
        }
    }

    /**
     * Gets the value for a key. If the value is not found, it is computed and added to the cache.
     *
     * @param key the key
     * @return the value
     * @throws IllegalArgumentException if the value is not found
     */
    public V get(K key) {
        long stamp = lock.tryOptimisticRead();
        V existing = cache.get(key);

        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                existing = cache.get(key);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        if (existing != null) {
            return existing;
        }

        stamp = lock.writeLock();

        try {
            final V newValue = valueComputer.apply(key);

            if (newValue == null) {
                throw new IllegalArgumentException("Computed a null value: key=" + key);
            }

            existing = cache.putIfAbsent(key, newValue);
            return existing == null ? newValue : existing;
        } finally {
            lock.unlockWrite(stamp);
        }
    }
}
