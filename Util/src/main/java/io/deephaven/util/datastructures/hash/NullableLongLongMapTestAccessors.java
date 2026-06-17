//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import io.deephaven.util.annotations.TestUseOnly;

@TestUseOnly
interface NullableLongLongMapTestAccessors extends NullableLongLongMap {
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
}
