//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LongStaticPercentileAverage and run "./gradlew replicateStaticPercentile" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.engine.table.impl.by.ssmpercentile.PercentileAverages;

/**
 * Selects the percentile of int values for {@link IntStaticPercentileOperator} when evenly divided percentiles are
 * averaged.
 */
final class IntStaticPercentileAverage {
    private IntStaticPercentileAverage() {}

    /**
     * When the low count, {@code (int) ((size - 1) * percentile) + 1}, is exactly half of the values, the result is the
     * average of the largest low value and the smallest high value; otherwise it is the largest low value.
     *
     * @param array the values, which are reordered
     * @param size the number of values in {@code array}, which must be positive
     * @param percentile the percentile to compute
     * @return the percentile of the values
     */
    static double averagedPercentile(final int[] array, final int size, final double percentile) {
        final int targetLo = (int) ((size - 1) * percentile) + 1;
        IntStaticPercentileOperator.select(array, size, targetLo - 1);
        if (targetLo == size - targetLo) {
            return PercentileAverages.average(array[targetLo - 1],
                    IntStaticPercentileOperator.min(array, targetLo, size));
        }
        return array[targetLo - 1];
    }
}
