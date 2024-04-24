package io.deephaven.time.calendar;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A cache that is designed to be fast when accessed concurrently. When created, the cache uses a ConcurrentHashMap to
 * store values. When the fast cache is enabled, the cache is converted to a HashMap. This is done because HashMap is
 * faster than {@link ConcurrentHashMap} when accessed concurrently. The fast cache is immutable, so it is safe to use
 * the HashMap for optimal concurrent access.
 * <p>
 * The fast cache population happens on a separate thread. This is done to avoid blocking the calling thread. The
 * calling thread can wait for the fast cache to be populated by calling {@link #enableFastCache} with the wait
 * parameter set to true. This will block the calling thread until the fast cache is populated.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
class FastConcurrentCache<K, V> {

    private final Function<K, V> valueComputer;
    private Map<K, V> cache = new ConcurrentHashMap<>();
    private volatile boolean fastCache = false;
    private CompletableFuture<Void> fastCacheFuture = null;

    /**
     * Creates a new cache.
     *
     * @param valueComputer computes the value for a key.
     */
    public FastConcurrentCache(final Function<K, V> valueComputer) {
        this.valueComputer = valueComputer;
    }

    /**
     * Returns whether the fast cache is enabled.
     *
     * @return whether the fast cache is enabled
     */
    public boolean isFastCache() {
        return fastCache;
    }

    /**
     * Enables the fast cache.
     * <p>
     * To enable the fast cache, a list of keys is provided. The value for each key is computed and added to the cache.
     * If the cache is already enabled, an exception is thrown. To regenerate the fast cache, clear the cache first.
     *
     * @param keys the keys to compute and add to the cache
     * @param wait whether to wait for the cache to be generated
     * @throws IllegalStateException if the fast cache is already enabled
     */
    void enableFastCache(final List<K> keys, final boolean wait) {

        synchronized (this) {
            if (fastCache) {
                throw new IllegalStateException(
                        "Fast cache is already enabled.  To change the range, clear the cache first.");
            }

            fastCacheFuture = CompletableFuture.runAsync(() -> {
                final Map<K, V> c = new HashMap<>(cache);

                for (K key : keys) {
                    if (!c.containsKey(key)) {
                        c.put(key, valueComputer.apply(key));
                    }
                }

                fastCache = true;
                cache = c;
                synchronized (this) {
                    fastCacheFuture = null;
                }
            });
        }

        if (wait) {
            fastCacheFuture.join();
        }
    }

    /**
     * Clears the cache and disables the fast cache.
     */
    void clear() {
        synchronized (this) {
            if (fastCacheFuture != null) {
                final boolean canceled = fastCacheFuture.cancel(true);

                if (!canceled) {
                    throw new IllegalStateException("Failed to cancel fast cache computation");
                }
            }
        }

        if (fastCache) {
            fastCache = false;
            cache = new ConcurrentHashMap<>();
        } else {
            cache.clear();
        }
    }

    /**
     * Gets the value for a key. If the fast cache is enabled, the value is retrieved from the cache. If the value is
     * not found, an exception is thrown. If the fast cache is not enabled, the value is computed and added to the
     * cache.
     *
     * @param key the key
     * @return the value
     * @throws IllegalArgumentException if the value is not found
     */
    public V get(K key) {
        if (fastCache) {
            final V v = cache.get(key);

            if (v == null) {
                throw new IllegalArgumentException(
                        "No value found for " + key + ".  Fast cache is enabled so the value can not be computed.");
            }

            return v;
        } else {
            return cache.computeIfAbsent(key, valueComputer);
        }
    }
}
