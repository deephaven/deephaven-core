//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

/**
 * Callback interface for propagating shifts over entire {@link RowSet RowSets}.
 */
public interface RowSetShiftCallback {
    /**
     * Signals that the row keys in {@code rowSet} should be shifted by the provided {@code shiftDelta}.
     *
     * @param rowSet The row keys to shift
     * @param shiftDelta The shift delta to apply to each row key in {@code rowSet}
     */
    void shift(RowSet rowSet, long shiftDelta);
}
