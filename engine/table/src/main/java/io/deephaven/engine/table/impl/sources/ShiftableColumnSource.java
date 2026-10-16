//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.WritableColumnSource;

public interface ShiftableColumnSource<T> extends WritableColumnSource<T> {
    /**
     * Update this column source according to the shift data. Positions the shift moves values away from, and does not
     * move others into, hold unspecified values afterward.
     *
     * @param shiftData the shift data to apply to this column source
     */
    void shift(RowSetShiftData shiftData);

    /**
     * Release the storage for every block that lies entirely within a range of row keys that hold no values. Partially
     * covered blocks are left alone.
     * <p>
     * A released block's current values are gone, and so are the previous values of its rows: a row not written during
     * the current cycle reads its previous value from the current block. Call this only once nothing can read either,
     * after the logical clock has completed the update cycle that removed the rows. A {@code TerminalNotification} runs
     * then. A concurrent snapshot that was still reading those rows' previous values fails its clock check and retries.
     * <p>
     * The caller must never write to a released block again: the capacity is unchanged, so {@code ensureCapacity} does
     * not allocate it.
     *
     * @param firstKey the first row key of the range
     * @param lastKey the last row key of the range, inclusive
     */
    void releaseBlocks(long firstKey, long lastKey);
}
