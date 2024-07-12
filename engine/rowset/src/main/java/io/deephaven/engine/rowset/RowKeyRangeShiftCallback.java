//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

/**
 * Functional interface to pass to {@link RowSetShiftData#apply(RowKeyRangeShiftCallback)} or
 * {@link RowSetShiftData#unapply(RowKeyRangeShiftCallback)} to get information about each shift recorded.
 */
@FunctionalInterface
public interface RowKeyRangeShiftCallback {
    /**
     * Process the shift.
     *
     * @param beginRange start of range (inclusive)
     * @param endRange end of range (inclusive)
     * @param shiftDelta amount range has moved by
     */
    void shift(long beginRange, long endRange, long shiftDelta);
}
