//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPercentileTypeMedianHelper and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.ssms.ShortSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class ShortPercentileTypeMedianHelper extends ShortPercentileTypeHelper {
    private final double percentile;
    private final DoubleArraySource resultColumn;

    ShortPercentileTypeMedianHelper(double percentile, WritableColumnSource resultColumn) {
        super(percentile, null);
        this.percentile = percentile;
        // region resultColumn
        this.resultColumn = (DoubleArraySource) resultColumn;
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
            final long targetLo = (int) ((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            if (ssmLo.totalSize() == ssmHi.totalSize()) {
                // region averageEvenlyDivided
                return setResult(destination, (((ShortSegmentedSortedMultiset) ssmLo).getMaxShort()
                        + ((ShortSegmentedSortedMultiset) ssmHi).getMinShort()) / 2.0);
                // endregion averageEvenlyDivided
            } else {
                return setResult(destination, ((ShortSegmentedSortedMultiset) ssmLo).getMaxShort());
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
