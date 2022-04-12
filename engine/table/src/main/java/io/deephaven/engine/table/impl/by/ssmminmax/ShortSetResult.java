/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetResult and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.engine.table.impl.ssms.ShortSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final ShortArraySource resultColumn;

    public ShortSetResult(boolean minimum, ArrayBackedColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (ShortArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final short newResult;
        if (ssm.size() == 0) {
            newResult = NULL_SHORT;
        } else {
            final ShortSegmentedSortedMultiset shortSsm = (ShortSegmentedSortedMultiset) ssm;
            newResult = minimum ? shortSsm.getMinShort() : shortSsm.getMaxShort();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_SHORT);
    }

    private boolean setResult(long destination, short newResult) {
        final short oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
