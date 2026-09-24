//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

/**
 * The interface we use for our Long2LongMaps that are the basis for a hashed redirection index.
 */
public interface NullableLongLongMap {
    /**
     * Empty the map and release its backing array. The next put allocates a new array at the initial capacity.
     */
    void resetToNull();

    /**
     * Empty the map and release its backing array, as {@link #resetToNull()} does, but remember the capacity the map
     * had reached so that the next allocation is made at that size instead of regrowing from the initial capacity
     * through successive rehashes. Like {@link #resetToNull()} and unlike {@link #clear()}, this never writes into the
     * array a concurrent reader might be probing, so it is safe to call while unsynchronized readers are active.
     */
    void resetToNullRetainingCapacity();

    int capacity();

    /**
     * @return the size of this map
     */
    int size();

    /**
     * @return true if this map is empty (i.e. size is zero)
     */
    boolean isEmpty();

    /**
     * @return the value returned from {@link #get(long)} when no value is present in the map.
     */
    long defaultReturnValue();

    /**
     * Add a mapping from key to value. Return the old value of key.
     * 
     * @param key the key to add
     * @param value the value to add
     * @return the old value of key (or {@link #defaultReturnValue()}} if there was no mapping)
     */
    long put(long key, long value);

    /**
     * Add a mapping from key to value, if one does not already exist. Return the old value of key (or
     * {@link #defaultReturnValue()}) if one does not exist.
     * 
     * @param key the key to add
     * @param value the value to add
     * @return the old value of key (or {@link #defaultReturnValue()}} if there was no mapping)
     */
    long putIfAbsent(long key, long value);

    /**
     * Gets the value associated with key. Returns {@link #defaultReturnValue()}} if no mapping exists.
     * 
     * @param key the key to get
     * @return the value of the key (or {@link #defaultReturnValue()})
     */
    long get(long key);

    /**
     * Remove a mapping for a key. Return the removed value of key (or {@link #defaultReturnValue()}) if one does not
     * exist.
     * 
     * @param key the key to add
     * @return the removed value of (or {@link #defaultReturnValue()}} if there was no mapping)
     */
    long remove(long key);

    /**
     * Empty the map in place, retaining its backing array and capacity. Not safe in the presence of concurrent readers.
     */
    void clear();

    void forEach(LongLongBiConsumer consumer);
}
