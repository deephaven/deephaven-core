//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.web.shared.data.RangeSet;

public class RowSetFactory {

    public static WritableRowSet empty() {
        return new WebRowSetImpl(RangeSet.empty());
    }
    public static RowSetBuilderSequential builderSequential() {
        return new WebRowSetBuilderSequentialImpl();
    }
    public static WritableRowSet fromRange(long first, long last) {
        return new WebRowSetImpl(RangeSet.ofRange(first, last));
    }
    public static WritableRowSet flat(long size) {
        return size <= 0 ? empty() : new WebRowSetImpl(RangeSet.ofRange(0, size - 1));
    }
}
