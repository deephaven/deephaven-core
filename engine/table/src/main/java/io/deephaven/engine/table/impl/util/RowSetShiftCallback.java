//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSet;

public interface RowSetShiftCallback {
    void shift(RowSet rowSet, long offset);
}
