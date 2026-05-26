//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.mutable.MutableInt;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

/**
 * The interface we use for our Long2LongMaps that are the basis for a hashed redirection index.
 */
public interface NullableLongLongMap {
    void resetToNull();

    int capacity();

    /**
     * Returns the keys of this map as a newly-allocated {@code long[]}. The array length equals {@link #size()}.
     *
     * <p>
     * Note that this method is unsafe in the face of concurrent modifications.
     * </p>
     */
    @TestUseOnly
    long[] keyArray();

    /**
     * Returns the keys of this map in {@code space}, if it is large enough; otherwise allocates a new array sized to
     * {@link #size()}.
     *
     * <p>
     * Note that this method is unsafe in the face of concurrent modifications.
     * </p>
     */
    @TestUseOnly
    long[] keyArray(long[] space);

    /**
     * Returns the values of this map as a newly-allocated {@code long[]}. The array length equals {@link #size()}.
     *
     * <p>
     * Note that this method is unsafe in the face of concurrent modifications.
     * </p>
     */
    @TestUseOnly
    long[] valueArray();

    /**
     * Returns the values of this map in {@code space}, if it is large enough; otherwise allocates a new array sized to
     * {@link #size()}.
     *
     * <p>
     * Note that this method is unsafe in the face of concurrent modifications.
     * </p>
     */
    @TestUseOnly
    long[] valueArray(long[] space);

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

    void clear();

    void forEach(LongLongBiConsumer consumer);
}
