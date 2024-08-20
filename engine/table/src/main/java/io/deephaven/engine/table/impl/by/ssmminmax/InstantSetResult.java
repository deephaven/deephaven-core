//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.InstantArraySource;
import io.deephaven.engine.table.impl.ssms.LongSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.util.QueryConstants;

public class InstantSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final InstantArraySource resultColumn;

    public InstantSetResult(boolean minimum, WritableColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (InstantArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final long newResult;
        if (ssm.size() == 0) {
            newResult = QueryConstants.NULL_LONG;
        } else {
            final LongSegmentedSortedMultiset longSsm = (LongSegmentedSortedMultiset) ssm;
            newResult = minimum ? longSsm.getMinLong() : longSsm.getMaxLong();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, QueryConstants.NULL_LONG);
    }

    private boolean setResult(long destination, long newResult) {
        final long oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
