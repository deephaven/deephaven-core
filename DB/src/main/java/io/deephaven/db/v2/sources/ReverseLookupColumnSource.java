/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import java.util.function.ToIntFunction;

/**
 * Common interface for column sources that provide a reverse-lookup function (value to int key). Note that int keys are
 * used because this is intended for column sources with a small, contiguous key range starting from 0 and well shorter
 * than Integer.MAX_VALUE.
 */
public interface ReverseLookupColumnSource<DATA_TYPE, EXTRA_VALUE_TYPE> extends ColumnSource<DATA_TYPE>,
        StringSetImpl.ReversibleLookup<DATA_TYPE> {
    /**
     * Get a reverse-lookup function for all non-null values stored in this column source at
     * {@code keys <= highestKeyNeeded}.
     *
     * @param highestKeyNeeded The highest key needed in the result map
     * @return A reverse-lookup function that has all values defined for keys in [0, highestKeyNeeded]
     */
    ToIntFunction<DATA_TYPE> getReverseLookup(final int highestKeyNeeded);

    /**
     * Get an implementation-defined "extra value" associated with this column source.
     */
    EXTRA_VALUE_TYPE getExtra();

    /**
     * Perform a reverse lookup
     *
     * @param highestIndex The highest key needed for the lookup
     * @param value The value we are looking up
     * @return The key, between 0 and highestIndex, for the value. A value outside this range if the value has no
     *         mapping in the range.
     */

    default int rget(int highestIndex, DATA_TYPE value) {
        return getReverseLookup(highestIndex).applyAsInt(value);
    }
}
