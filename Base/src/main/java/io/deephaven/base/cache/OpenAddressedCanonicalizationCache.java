/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.cache;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import gnu.trove.impl.PrimeFinder;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * An object canonicalization cache, suitable for use with objects that define equals(...) in such a
 * way as to identify objects that can be mutually substituted in a manner appropriate for the
 * application using the cache. Objects with an improper hashCode() implementation will cause
 * undefined behavior.
 *
 * See KeyedObjectHashMap and its parent classes for many of the ideas I'm working from. The
 * implementation is (loosely) based on an open-addressed hash map.
 *
 * The intended problem domain is effectively single-threaded, so I've optimized on single-threaded
 * efficiency and used coarse synchronization instead of optimizing for concurrency.
 */
public class OpenAddressedCanonicalizationCache {

    /**
     * Allows cache users to supercede the equals() and hashCode() methods of their input items, and
     * supply an alternative object to cache.
     * 
     * @param <INPUT_TYPE>
     * @param <OUTPUT_TYPE>
     */
    public interface Adapter<INPUT_TYPE, OUTPUT_TYPE> {
        /**
         * Note: equals(inputItem, cachedItem) implies hashCode(inputItem) == cachedItem.hashCode()
         * must be true.
         * 
         * @param inputItem The input item
         * @param cachedItem The cached item
         * @return True if inputItem is equal to cachedItem for the cache's purposes.
         */
        boolean equals(@NotNull final INPUT_TYPE inputItem, @NotNull final Object cachedItem);

        /**
         * @param inputItem The input item
         * @return Return a hash code for inputItem consistent with its cacheable form.
         */
        int hashCode(@NotNull final INPUT_TYPE inputItem);

        /**
         * Note: The following must be true: hashCode(inputItem) == outputItem.hashCode() &&
         * equals(inputItem, outputItem)
         * 
         * @param inputItem The input item
         * @return A cacheable version of inputItem.
         */
        OUTPUT_TYPE makeCacheableItem(@NotNull final INPUT_TYPE inputItem);
    }

    private static class DefaultAdapter implements Adapter<Object, Object> {

        @Override
        public boolean equals(@NotNull Object inputItem, @NotNull Object cachedItem) {
            return inputItem.equals(cachedItem);
        }

        @Override
        public int hashCode(@NotNull Object inputItem) {
            return inputItem.hashCode();
        }

        @Override
        public Object makeCacheableItem(@NotNull Object inputItem) {
            return inputItem;
        }
    }

    private static Adapter<?, ?> DEFAULT_ADAPTER = new DefaultAdapter();

    private static class ItemReference<T> extends WeakReference<T> {

        private boolean reclaimed = false;

        ItemReference(@NotNull final T referent, final ReferenceQueue<? super T> queue) {
            super(referent, queue);
        }

        boolean reclaimed() {
            return reclaimed;
        }

        void markReclaimed() {
            reclaimed = true;
        }
    }

    private final float loadFactor;

    private ItemReference<?> storage[];
    private int occupancyThreshold;
    private int occupiedSlots;
    private int emptySlots;

    private final ReferenceQueue<Object> cleanupQueue = new ReferenceQueue<>();

    @SuppressWarnings("WeakerAccess")
    public OpenAddressedCanonicalizationCache(final int minimumInitialCapacity,
        final float loadFactor) {
        this.loadFactor = Require.inRange(loadFactor, 0.0f, 1.0f, "loadFactor");
        initialize(computeInitialCapacity(minimumInitialCapacity, loadFactor));
    }

    public OpenAddressedCanonicalizationCache(final int minimumInitialCapacity) {
        this(minimumInitialCapacity, 0.5f);
    }

    @SuppressWarnings("unused")
    public OpenAddressedCanonicalizationCache() {
        this(10, 0.5f);
    }

    private void initialize(final int capacity) {
        storage = new ItemReference[capacity];
        occupancyThreshold = computeOccupancyThreshold(capacity, loadFactor);
        occupiedSlots = 0;
        emptySlots = capacity;
    }

    /**
     * Note: Intended for unit test use only.
     * 
     * @return The threshold that occupancy must exceed to trigger a rehash
     */
    int getOccupancyThreshold() {
        return occupancyThreshold;
    }

    /**
     * Note: Intended for unit test use only.
     * 
     * @return The number of items in the cache (may be briefly larger, if the cleanupQueue needs to
     *         be drained)
     */
    int getOccupiedSlots() {
        return occupiedSlots;
    }

    public synchronized <INPUT_OUTPUT_TYPE> INPUT_OUTPUT_TYPE getCachedItem(
        @NotNull final INPUT_OUTPUT_TYPE item) {
        // noinspection unchecked
        return getCachedItem(item, (Adapter<INPUT_OUTPUT_TYPE, INPUT_OUTPUT_TYPE>) DEFAULT_ADAPTER);
    }

    public synchronized <INPUT_TYPE, OUTPUT_TYPE> OUTPUT_TYPE getCachedItem(
        @NotNull final INPUT_TYPE item, @NotNull final Adapter<INPUT_TYPE, OUTPUT_TYPE> adapter) {
        cleanup();
        return getOrInsertCachedItem(item, adapter);
    }

    private void cleanup() {
        ItemReference<?> itemReference;
        while ((itemReference = (ItemReference<?>) cleanupQueue.poll()) != null) {
            Assert.eqNull(itemReference.get(), "itemReference.get()");
            maybeReclaim(itemReference);
        }
    }

    private void maybeReclaim(@NotNull final ItemReference<?> itemReference) {
        if (!itemReference.reclaimed()) {
            --occupiedSlots;
            itemReference.markReclaimed();
        }
    }

    private <INPUT_TYPE, OUTPUT_TYPE> OUTPUT_TYPE getOrInsertCachedItem(
        @NotNull final INPUT_TYPE item, @NotNull final Adapter<INPUT_TYPE, OUTPUT_TYPE> adapter) {
        final int length = storage.length;
        final int hashCode = adapter.hashCode(item) & 0x7FFFFFFF;
        final int probeInterval = computeProbeInterval(hashCode, length);

        int slot = hashCode % length;
        int firstDeletedSlot = -1;
        do {
            final ItemReference<?> candidateReference = storage[slot];
            if (candidateReference == null) {
                final OUTPUT_TYPE cacheableItem = adapter.makeCacheableItem(item);
                // Assert.eq(hashCode, "hashCode", cacheableItem.hashCode(),
                // "cacheableItem.hashCode()");
                // Assert.assertion(adapter.equals(item, cacheableItem), "adapter.equals(item,
                // cacheableItem)");
                if (firstDeletedSlot == -1) {
                    --emptySlots;
                    storage[slot] = new ItemReference<>(cacheableItem, cleanupQueue);
                } else {
                    storage[firstDeletedSlot] = new ItemReference<>(cacheableItem, cleanupQueue);
                }
                ++occupiedSlots;
                maybeRehash();
                return cacheableItem;
            }

            final Object candidate = candidateReference.get();
            if (candidate == null) {
                if (firstDeletedSlot == -1) {
                    firstDeletedSlot = slot;
                }
                maybeReclaim(candidateReference);
            } else if (adapter.equals(item, candidate)) {
                // noinspection unchecked
                return (OUTPUT_TYPE) candidate;
            }

            if ((slot -= probeInterval) < 0) {
                slot += length;
            }
        } while (true);
    }

    private void maybeRehash() {
        final int newCapacity;
        if (occupiedSlots > occupancyThreshold) {
            newCapacity = computeNextCapacity(storage.length);
        }
        // Can go all the way to 0, since we don't have concurrent gets to worry about.
        else if (emptySlots == 0) {
            newCapacity = storage.length;
        } else {
            return;
        }
        rehash(newCapacity);
        cleanup();
    }

    private void rehash(final int newCapacity) {
        final ItemReference<?> oldStorage[] = storage;

        initialize(newCapacity);

        for (final ItemReference<?> itemReference : oldStorage) {
            if (itemReference == null || itemReference.reclaimed()) {
                continue;
            }
            final Object item = itemReference.get();
            if (item != null) {
                insertReferenceForRehash(itemReference, item);
            } else if (!itemReference.reclaimed()) {
                // NB: We don't need to decrement occupiedSlots here - we're instead not
                // incrementing it.
                itemReference.markReclaimed();
            }
        }
    }

    private void insertReferenceForRehash(final ItemReference<?> itemReference, final Object item) {
        final int length = storage.length;
        final int hashCode = item.hashCode() & 0x7FFFFFFF;
        final int probeInterval = computeProbeInterval(hashCode, length);

        int slot = hashCode % length;
        int firstDeletedSlot = -1;
        do {
            final ItemReference<?> candidateReference = storage[slot];
            if (candidateReference == null) {
                if (firstDeletedSlot == -1) {
                    --emptySlots;
                    storage[slot] = itemReference;
                } else {
                    storage[firstDeletedSlot] = itemReference;
                }
                ++occupiedSlots;
                return;
            }

            if (candidateReference.get() == null) {
                if (firstDeletedSlot == -1) {
                    firstDeletedSlot = slot;
                }
                maybeReclaim(candidateReference);
            }
            // NB: No need to test if item.equals(candidate) here - should be impossible during a
            // rehash.

            if ((slot -= probeInterval) < 0) {
                slot += length;
            }
        } while (true);

    }

    private static int computeInitialCapacity(final int minimumInitialCapacity,
        final float loadFactor) {
        return PrimeFinder.nextPrime((int) Math.ceil(minimumInitialCapacity / loadFactor) + 1);
    }

    private static int computeNextCapacity(final int capacity) {
        return PrimeFinder.nextPrime(capacity << 1);
    }

    private static int computeOccupancyThreshold(final int capacity, final float loadFactor) {
        return Math.min(capacity - 1, (int) Math.floor(capacity * loadFactor));
    }

    private static int computeProbeInterval(final int hashCode, final int length) {
        // Stolen directly from KeyedObjectHash
        // see Knuth, p. 529
        return 1 + (hashCode % (length - 2));
    }
}
