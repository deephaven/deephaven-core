//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

public interface WritableRowSet extends RowSet {
    void insert(long key);
    void insert(RowSet rowSet);

    void shiftInPlace(long shiftAmount);

    void retain(RowSet rowSetToIntersect);

    void removeRange(long startKey, long endKey);
    void remove(RowSet removed);
}
