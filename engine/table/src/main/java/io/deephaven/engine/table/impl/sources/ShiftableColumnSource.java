//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.WritableColumnSource;

public interface ShiftableColumnSource<T> extends WritableColumnSource<T> {
    /**
     * Update this column source according to the shift data.
     * 
     * @param shiftData the shift data to apply to this column source
     */
    void shift(RowSetShiftData shiftData);

    /**
     * Release the storage for every block that lies entirely within a range of row keys that will never be read or
     * written again. Partially covered blocks are left alone. The values in a released block must not be accessed,
     * including as previous values, so this may only be called once the update cycle that removed those rows has
     * completed.
     *
     * @param firstKey the first row key of the range
     * @param lastKey the last row key of the range, inclusive
     */
    default void releaseBlocks(long firstKey, long lastKey) {}
}
