package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;
import io.deephaven.engine.table.impl.ssms.LongSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;


public class DateTimeSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final DateTimeArraySource resultColumn;

    public DateTimeSetResult(boolean minimum, ArrayBackedColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (DateTimeArraySource) resultColumn;
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
