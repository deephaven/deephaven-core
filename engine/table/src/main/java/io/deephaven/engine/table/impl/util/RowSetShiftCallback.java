package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSet;

public interface RowSetShiftCallback {
    void shift(RowSet rowSet, long offset);
}
