//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

public interface WritableRowSet extends RowSet {
    WritableRowSet shift(long shiftAmount);
}
