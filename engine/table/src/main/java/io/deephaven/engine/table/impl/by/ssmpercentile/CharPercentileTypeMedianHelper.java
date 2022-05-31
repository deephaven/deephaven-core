package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.ssms.CharSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class CharPercentileTypeMedianHelper extends CharPercentileTypeHelper {
    private final double percentile;
    private final DoubleArraySource resultColumn;

    CharPercentileTypeMedianHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        super(percentile, null);
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (DoubleArraySource)resultColumn;
        // endregion
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final double newResult;
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            newResult = NULL_DOUBLE;
        } else {
            final long targetLo = (int)((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            if (ssmLo.totalSize() == ssmHi.totalSize()) {
                // region averageEvenlyDivided
                return setResult(destination, (((CharSegmentedSortedMultiset)ssmLo).getMaxChar() + ((CharSegmentedSortedMultiset)ssmHi).getMinChar()) / 2.0);
                // endregion averageEvenlyDivided
            } else {
                return setResult(destination, ((CharSegmentedSortedMultiset)ssmLo).getMaxChar());
            }
        }
        return setResult(destination, newResult);
    }

    @Override
    public boolean setResultNull(long destination) {
        return setResult(destination, NULL_DOUBLE);
    }

    private boolean setResult(long destination, double newResult) {
        final double oldResult = resultColumn.getAndSetUnsafe(destination, newResult);
        return oldResult != newResult;
    }
}