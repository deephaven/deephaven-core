//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatPercentileTypeMedianHelper and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoublePercentileTypeMedianHelper extends DoublePercentileTypeHelper {
    private final double percentile;
    private final DoubleArraySource resultColumn;

    DoublePercentileTypeMedianHelper(double percentile, WritableColumnSource resultColumn) {
        super(percentile, resultColumn);
        this.percentile = percentile;
        this.resultColumn = (DoubleArraySource) resultColumn;
    }

    @Override
    public boolean setResult(SegmentedSortedMultiSet ssmLo, SegmentedSortedMultiSet ssmHi, long destination) {
        final long loSize = ssmLo.totalSize();
        final long hiSize = ssmHi.totalSize();
        final long totalSize = loSize + hiSize;

        if (totalSize == 0) {
            return setResult(destination, NULL_DOUBLE);
        } else {
            final long targetLo = (int) ((totalSize - 1) * percentile) + 1;
            if (loSize < targetLo) {
                ssmHi.moveFrontToBack(ssmLo, targetLo - loSize);
            } else if (loSize > targetLo) {
                ssmLo.moveBackToFront(ssmHi, loSize - targetLo);
            }

            if (ssmLo.totalSize() == ssmHi.totalSize()) {
                final double divisor = (double) 2.0;
                return setResult(destination, (((DoubleSegmentedSortedMultiset) ssmLo).getMaxDouble()
                        + ((DoubleSegmentedSortedMultiset) ssmHi).getMinDouble()) / divisor);
            } else {
                return setResult(destination, ((DoubleSegmentedSortedMultiset) ssmLo).getMaxDouble());
            }
        }
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
