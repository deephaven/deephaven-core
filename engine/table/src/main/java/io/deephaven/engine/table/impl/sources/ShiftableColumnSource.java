//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
}
