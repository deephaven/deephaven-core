//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.hash.KeyedIntObjectHash;
import io.deephaven.hash.KeyedIntObjectKey;

import java.util.function.IntFunction;

/**
 * A cache that is designed to be fast when accessed concurrently with read-heavy workloads. Values are populated from a
 * function when they are not found in the cache. All values must be non-null.
 *
 * @param <V> the value type
 */
class ReadOptimizedConcurrentCache<V extends ReadOptimizedConcurrentCache.IntKeyedValue> {

    /**
     * A value that has an included integer key.
     */
    public static abstract class IntKeyedValue {
        private final int key;

        /**
         * Creates a new value.
         *
         * @param key the key
         */
        public IntKeyedValue(int key) {
            this.key = key;
        }

        /**
         * Gets the key.
         *
         * @return the key
         */
        public int getKey() {
            return key;
        }
    }

    /**
     * A pair of a key and a value.
     *
     * @param <T> the value type
     */
    public static class Pair<T> extends IntKeyedValue {
        private final T value;

        /**
         * Creates a new pair.
         *
         * @param key the key
         * @param value the value
         */
        public Pair(int key, T value) {
            super(key);
            this.value = value;
        }

        /**
         * Gets the value.
         *
         * @return the value
         */
        public T getValue() {
            return value;
        }
    }

    private static class KeyDef<V extends IntKeyedValue> extends KeyedIntObjectKey.BasicStrict<V> {

        @Override
        public int getIntKey(V v) {
            return v.getKey();
        }

    }

    private final IntFunction<V> valueComputer;
    private final KeyedIntObjectHash<V> cache;

    /**
     * Creates a new cache.
     *
     * @param initialCapacity the initial capacity
     * @param valueComputer computes the value for a key.
     */
    public ReadOptimizedConcurrentCache(final int initialCapacity, final IntFunction<V> valueComputer) {
        this.valueComputer = valueComputer;
        this.cache = new KeyedIntObjectHash<>(initialCapacity, new KeyDef<>());
    }

    /**
     * Clears the cache.
     */
    synchronized void clear() {
        cache.clear();
    }

    /**
     * Gets the value for a key. If the value is not found, it is computed and added to the cache.
     *
     * @param key the key
     * @return the value
     * @throws NullPointerException if the value is not found
     */
    public V computeIfAbsent(int key) {
        V existing = cache.get(key);

        if (existing != null) {
            return existing;
        }

        final V newValue = valueComputer.apply(key);

        if (newValue == null) {
            throw new NullPointerException("Computed a null value: key=" + key);
        }

        cache.put(key, newValue);
        return newValue;
    }
}
