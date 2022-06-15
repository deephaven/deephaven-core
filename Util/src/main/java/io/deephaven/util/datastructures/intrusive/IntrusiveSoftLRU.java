/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.datastructures.intrusive;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A Caching structure that uses simple arrays with doubling, until a threshold is reached. It then switches to an LRU
 * cache beyond the maximum size.
 *
 * @param <T> the cached type
 */
public class IntrusiveSoftLRU<T> {
    private static final SoftReference<?> NULL = new SoftReference<>(null);

    private final Adapter<T> adapter;
    private final int maxCapacity;

    private int size;
    private int[] nexts;
    private int[] prevs;
    private SoftReference<T>[] softReferences;

    private int tail;

    private static <T> SoftReference<T> getNull() {
        // noinspection unchecked
        return (SoftReference<T>) NULL;
    }

    /**
     * Create a cache object with the specified initial capacity. The structure will switch to LRU mode once maxCapacity
     * has been reached.
     *
     * @param adapter the {@link Adapter} object for setting the intrusive properties.
     * @param initialCapacity the initial size of the cache.
     * @param maxCapacity the maximum capacity of the cache.
     */
    public IntrusiveSoftLRU(@NotNull final Adapter<T> adapter, final int initialCapacity, final int maxCapacity) {
        Assert.gt(maxCapacity, "maxCapacity", 1);
        Assert.leq(initialCapacity, "initialCapacity", maxCapacity, "maxCapacity");

        this.maxCapacity = maxCapacity;
        this.adapter = adapter;
        this.tail = 0;
        this.size = 0;

        if (initialCapacity == maxCapacity) {
            initializeLinks();
        }

        // noinspection unchecked
        softReferences = new SoftReference[initialCapacity];
        Arrays.fill(softReferences, getNull());
    }

    /**
     * Touch an item in the cache. If it has not been visited yet, it will be added, potentially dropping the least
     * recently accessed item in the cache.
     *
     * @param itemToTouch the item to touch.
     */
    public synchronized void touch(@NotNull final T itemToTouch) {
        if (softReferences.length == maxCapacity) {
            touchLRU(itemToTouch);
        } else {
            touchSimple(itemToTouch);
        }
    }

    /**
     * Clear all item from the cache.
     */
    public synchronized void clear() {
        Arrays.fill(softReferences, getNull());
    }

    /**
     * Check the current capacity of the cache.
     * 
     * @return the current capacity
     */
    @TestUseOnly
    int currentCapacity() {
        return softReferences.length;
    }

    /**
     * Initialize the next and prev links for LRU mode.
     */
    private void initializeLinks() {
        size = maxCapacity;
        nexts = new int[maxCapacity];
        prevs = new int[maxCapacity];
        nexts[0] = maxCapacity - 1;

        for (int i = 1; i < maxCapacity; ++i) {
            nexts[i] = i - 1;
            prevs[i - 1] = i;
        }

        prevs[maxCapacity - 1] = 0;
    }

    /**
     * Touch the item in LRU mode.
     *
     * @param itemToTouch the item to touch
     */
    private void touchLRU(@NotNull T itemToTouch) {
        final int slot = adapter.getSlot(itemToTouch);

        if (slot < 0 ||
                slot >= currentCapacity() ||
                softReferences[slot].get() != itemToTouch) {
            // A new entry, place it on top of the tail
            adapter.setSlot(itemToTouch, tail);
            softReferences[tail] = adapter.getOwner(itemToTouch);

            // Move the tail back one slot
            tail = prevs[tail];
        } else if (slot != tail) {
            // We already have an entry, we just move it to the head
            final int head = nexts[tail];

            if (slot != head) {
                // Unsplice this entry from the list
                int prev = prevs[slot];
                int next = nexts[slot];

                nexts[prev] = next;
                prevs[next] = prev;

                // Move this entry to be the new head
                nexts[slot] = head;
                nexts[tail] = slot;

                prevs[head] = slot;
                prevs[slot] = tail;
            }
        } else {
            // Just move the tail back one slot, making the entry the head
            tail = prevs[tail];
        }
    }

    /**
     * Simply add the item to the array if it is new, potentially resizing if needed and switching to LRU mode.
     *
     * @param itemToTouch the item to touch
     */
    private void touchSimple(@NotNull final T itemToTouch) {
        final int slot = adapter.getSlot(itemToTouch);
        if (slot < 0 ||
                slot >= currentCapacity() ||
                softReferences[slot].get() != itemToTouch) {
            // We'll have to add it, we haven't seen it before. First, do we need to resize?
            if (size < softReferences.length) {
                // No, cool. Just add it.
                final int slotForItem = size++;
                adapter.setSlot(itemToTouch, slotForItem);
                softReferences[slotForItem] = adapter.getOwner(itemToTouch);
            } else {
                // Yep, resize, copy, and potentially switch to LRU mode.
                doResize();
                touch(itemToTouch);
            }
        }
    }

    /**
     * Resize the cache by doubling, stopping at the maximum capacity. If maximum capacity is reached, then switch the
     * structure into LRU mode.
     */
    private void doResize() {
        // First we will try to compact the references. Maybe we can get away without actually resizing
        int nullsFound = 0;
        for (int refIdx = 0; refIdx < softReferences.length; refIdx++) {
            final T object = softReferences[refIdx].get();
            if (object == null) {
                nullsFound++;
            } else if (nullsFound > 0) {
                softReferences[refIdx - nullsFound] = softReferences[refIdx];
                adapter.setSlot(object, refIdx - nullsFound);
            }
        }

        if (nullsFound > 0) {
            // We free'd up space! Cool!
            size -= nullsFound;
            Assert.lt(size, "size", softReferences.length, "softReferences.length");
            Arrays.fill(softReferences, size, softReferences.length, getNull());
            return;
        }

        // Time to reallocate or change internal structures
        final int oldCapacity = softReferences.length;
        final int newCapacity = Math.min(oldCapacity << 1, maxCapacity);
        softReferences = Arrays.copyOf(softReferences, newCapacity);
        Arrays.fill(softReferences, oldCapacity, newCapacity, getNull());
        if (newCapacity == maxCapacity) {
            initializeLinks();
        }
    }

    @VisibleForTesting
    synchronized boolean verifyLRU(int numStored) {
        final Set<T> refsVisited = new HashSet<>();
        final TIntSet visited = new TIntHashSet();

        int v = tail;

        for (int i = 0; i < nexts.length; ++i) {
            // We should never visit the same position twice. We should also
            // never encounter the same object twice.
            if (!visited.add(v) || !refsVisited.add(softReferences[v].get())) {
                return false;
            }
            v = nexts[v];
        }

        if (v != tail) {
            return false;
        }

        if (visited.size() != numStored) {
            return false;
        }

        for (int i = 0; i < prevs.length; ++i) {
            if (!visited.remove(v)) {
                return false;
            }
            v = prevs[v];
        }

        return v == tail;
    }

    /**
     * Evict an item from the cache by nulling it's internal reference. Intended for test use only.
     * 
     * @param refIdx the index to evict
     */
    @TestUseOnly
    void evict(int refIdx) {
        if (refIdx < 0 || refIdx > softReferences.length || softReferences[refIdx] == getNull()) {
            return;
        }

        softReferences[refIdx].clear();
    }

    @TestUseOnly
    int size() {
        return size;
    }

    /**
     * An interface defining the required intrusive property getters and setters. Users should not directly call these
     * methods, or they risk corrupting the internal data structure.
     * 
     * @param <T>
     */
    public interface Adapter<T> {

        /**
         * Get a {@link SoftReference} object which refers to the object being cached and will act as its "owner" in the
         * cache.
         *
         * @param cachedObject the object being cached.
         * @return a {@link SoftReference} that refers to the item and will act as its "owner" in the cache.
         */
        SoftReference<T> getOwner(T cachedObject);

        /**
         * Get the slot in which the object is stored in the cache.
         *
         * @param cachedObject the being cached.
         * @return the slot for the object.
         */
        int getSlot(T cachedObject);

        /**
         * Set the slot in which the reference is stored in the cache.
         *
         * @param cachedObject the object being cached.
         * @param slot the slot where it will be stored.
         */
        void setSlot(T cachedObject, int slot);
    }

    /**
     * An intrusive node for storing items in the cache.
     * 
     * @param <T> the actual node type
     */
    public interface Node<T extends Node<T>> {

        SoftReference<T> getOwner();

        int getSlot();

        void setSlot(int slot);

        /**
         * The base node implementation for items in the cache.
         * 
         * @param <T>
         */
        class Impl<T extends Impl<T>> implements Node<T> {

            private final SoftReference<T> owner;
            private int slot;

            protected Impl() {
                // noinspection unchecked
                owner = new SoftReference<>((T) this);
                slot = -1;
            }

            @Override
            public SoftReference<T> getOwner() {
                return owner;
            }

            @Override
            public int getSlot() {
                return slot;
            }

            @Override
            public void setSlot(int slot) {
                this.slot = slot;
            }
        }

        /**
         * A basic adapter class for {@link Node} items in the cache.
         * 
         * @param <T>
         */
        class Adapter<T extends Node<T>> implements IntrusiveSoftLRU.Adapter<T> {

            private static final IntrusiveSoftLRU.Adapter<?> INSTANCE = new Adapter<>();

            public static <T extends Node<T>> IntrusiveSoftLRU.Adapter<T> getInstance() {
                // noinspection unchecked
                return (IntrusiveSoftLRU.Adapter<T>) INSTANCE;
            }

            @Override
            public SoftReference<T> getOwner(@NotNull T cachedObject) {
                return cachedObject.getOwner();
            }

            @Override
            public int getSlot(@NotNull T cachedObject) {
                return cachedObject.getSlot();
            }

            @Override
            public void setSlot(@NotNull T cachedObject, int slot) {
                cachedObject.setSlot(slot);
            }
        }
    }

}
