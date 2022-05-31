/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetResult and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.ssms.FloatSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final FloatArraySource resultColumn;

    public FloatSetResult(boolean minimum, ArrayBackedColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (FloatArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final float newResult;
        if (ssm.size() == 0) {
            newResult = NULL_FLOAT;
        } else {
            final FloatSegmentedSortedMultiset floatSsm = (FloatSegmentedSortedMultiset) ssm;
            newResult = minimum ? floatSsm.getMinFloat() : floatSsm.getMaxFloat();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_FLOAT);
    }

    private boolean setResult(long destination, float newResult) {
        final float oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
