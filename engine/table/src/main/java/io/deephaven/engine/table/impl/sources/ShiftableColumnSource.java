//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.WritableColumnSource;

public interface ShiftableColumnSource<T> extends WritableColumnSource<T> {
    /**
     * Update this column source according to the shift data. Positions the shift moves values away from, and does not
     * move others into, hold no values afterward; an array-backed source may leave their storage unallocated.
     *
     * @param shiftData the shift data to apply to this column source
     */
    void shift(RowSetShiftData shiftData);

    /**
     * Release the storage for every block that lies entirely within a range of row keys that hold no values. Partially
     * covered blocks are left alone. The values in a released block must not be accessed, including as previous values,
     * so this may only be called once the update cycle that removed those rows has completed. A released block is not
     * written again unless the range reached the end of the capacity: then the capacity shrinks to the start of the
     * released blocks, and {@code ensureCapacity} allocates them again.
     *
     * @param firstKey the first row key of the range
     * @param lastKey the last row key of the range, inclusive
     */
    default void releaseBlocks(long firstKey, long lastKey) {}
}
