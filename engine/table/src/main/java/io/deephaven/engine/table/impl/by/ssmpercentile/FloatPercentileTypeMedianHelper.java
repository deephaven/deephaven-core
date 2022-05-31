package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.ssms.FloatSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatPercentileTypeMedianHelper extends FloatPercentileTypeHelper {
    private final double percentile;
    private final FloatArraySource resultColumn;

    FloatPercentileTypeMedianHelper(double percentile, ArrayBackedColumnSource resultColumn) {
        super(percentile, resultColumn);
        this.percentile = percentile;
        this.resultColumn = (FloatArraySource)resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_FLOAT);
        } else {
            final long targetLo = (int)((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            if (ssmLo.totalSize() == ssmHi.totalSize()) {
                final float divisor = (float)2.0;
                return setResult(destination, (((FloatSegmentedSortedMultiset)ssmLo).getMaxFloat() + ((FloatSegmentedSortedMultiset)ssmHi).getMinFloat()) / divisor);
            } else {
                return setResult(destination, ((FloatSegmentedSortedMultiset)ssmLo).getMaxFloat());
            }
        }
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