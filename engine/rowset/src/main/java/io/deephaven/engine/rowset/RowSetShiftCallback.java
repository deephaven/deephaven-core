//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

/**
 * Implementing this indicates that instances should be informed when shifts occur to perform their own bookkeeping as
 * necessary.
 */
public interface RowSetShiftCallback {
    /**
     * Signals that the range should be shifted by the provided offset.
     *
     * @param rowSet The keys to shift
     * @param offset The offset to move each key by
     */
    void shift(RowSet rowSet, long offset);
}
