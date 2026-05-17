//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import it.unimi.dsi.fastutil.longs.Long2LongMap;

public interface NullableLong2LongMap extends Long2LongMap {
    void resetToNull();

    int capacity();

    /**
     * Returns the keys of this map as a newly-allocated {@code long[]}. The array length equals {@link #size()}.
     */
    long[] keyArray();

    /**
     * Returns the keys of this map in {@code space}, if it is large enough; otherwise allocates a new array sized to
     * {@link #size()}.
     */
    long[] keyArray(long[] space);

    /**
     * Returns the values of this map as a newly-allocated {@code long[]}. The array length equals {@link #size()}.
     */
    long[] valueArray();

    /**
     * Returns the values of this map in {@code space}, if it is large enough; otherwise allocates a new array sized to
     * {@link #size()}.
     */
    long[] valueArray(long[] space);
}
