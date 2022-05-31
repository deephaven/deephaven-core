package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.BooleanArraySource;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;


public class BooleanSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final BooleanArraySource resultColumn;

    public BooleanSetResult(boolean minimum, ArrayBackedColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (BooleanArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final Boolean newResult;
        if (ssm.size() == 0) {
            newResult = null;
        } else {
            final ObjectSegmentedSortedMultiset objectSsm = (ObjectSegmentedSortedMultiset) ssm;
            newResult = (Boolean)(minimum ? objectSsm.getMinObject() : objectSsm.getMaxObject());
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, null);
    }

    private boolean setResult(long destination, Boolean newResult) {
        final byte newAsByte = BooleanUtils.booleanAsByte(newResult);
        final byte oldResult = resultColumn.getAndSetUnsafe(destination, newAsByte);
        return oldResult != newAsByte;
    }
}
