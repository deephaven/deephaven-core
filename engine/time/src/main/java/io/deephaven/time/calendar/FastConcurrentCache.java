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
    private final Map<K, V> concurrentCache = new ConcurrentHashMap<>();
    private final Map<K, V> fastCache = new HashMap<>();
    private boolean isFastCacheEnabled = false; // synchronized
    private volatile boolean isFastCacheActive = false;
    private CompletableFuture<Void> fastCacheFuture = null; // synchronized

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
    public synchronized boolean isFastCache() {
        return isFastCacheEnabled;
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
    synchronized void enableFastCache(final List<K> keys, final boolean wait) {
        if (isFastCacheEnabled) {
            throw new IllegalStateException(
                    "Fast cache is already enabled.  To change the range, clear the cache first.");
        }

        isFastCacheEnabled = true;

        fastCacheFuture = CompletableFuture.runAsync(() -> {
            fastCache.putAll(concurrentCache);

            for (K key : keys) {
                if (!fastCache.containsKey(key)) {
                    fastCache.put(key, valueComputer.apply(key));
                }
            }

            isFastCacheActive = true;
        });

        if (wait) {
            fastCacheFuture.join();
        }
    }

    /**
     * Clears the cache and disables the fast cache.
     */
    synchronized void clear() {
        if (fastCacheFuture != null) {
            if (!fastCacheFuture.isDone() && !fastCacheFuture.cancel(true)) {
                throw new IllegalStateException("Failed to cancel fast cache computation");
            }
        }

        if (isFastCacheActive) {
            isFastCacheActive = false;
        }

        isFastCacheEnabled = false;
        fastCache.clear();
        concurrentCache.clear();
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
        if (isFastCacheActive) {
            final V v = fastCache.get(key);

            if (v == null) {
                throw new IllegalArgumentException(
                        "No value found for " + key + ".  Fast cache is enabled so the value can not be computed.");
            }

            return v;
        } else {
            return concurrentCache.computeIfAbsent(key, valueComputer);
        }
    }
}
