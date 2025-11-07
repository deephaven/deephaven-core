//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.datastructures.LongRangeConsumer;

public interface RowSetBuilderSequential extends LongRangeConsumer {
    void appendRange(long rangeFirstRowKey, long rangeLastRowKey);
    RowSet build();
}
