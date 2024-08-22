//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;

public interface RowSet extends RowSequence, LongSizedDataStructure, SafeCloseable {
    RowSet copy();
    long get(long rowPosition);

    WritableRowSet intersect(RowSet rowSet);
}
