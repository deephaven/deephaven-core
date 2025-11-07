//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.web.shared.data.RangeSet;

public class RowSequenceFactory {
    public static final RowSequence EMPTY = new WebRowSetImpl(RangeSet.empty());
    public static RowSequence forRange(final long firstRowKey, final long lastRowKey) {
        return new WebRowSetImpl(RangeSet.ofRange(firstRowKey, lastRowKey));
    }
}
