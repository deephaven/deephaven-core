package io.deephaven.engine.table.impl.by.ssmminmax;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.impl.ssms.CharSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

public class CharSetResult implements SsmChunkedMinMaxOperator.SetResult {
    private final boolean minimum;
    private final CharacterArraySource resultColumn;

    public CharSetResult(boolean minimum, ArrayBackedColumnSource resultColumn) {
        this.minimum = minimum;
        this.resultColumn = (CharacterArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssm, long destination) {
        final char newResult;
        if (ssm.size() == 0) {
            newResult = NULL_CHAR;
        } else {
            final CharSegmentedSortedMultiset charSsm = (CharSegmentedSortedMultiset) ssm;
            newResult = minimum ? charSsm.getMinChar() : charSsm.getMaxChar();
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_CHAR);
    }

    private boolean setResult(long destination, char newResult) {
        final char oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}
