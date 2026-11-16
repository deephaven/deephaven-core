//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import java.math.BigInteger;

/**
 * The average of the two values on either side of an evenly divided percentile, computed without overflow. When the sum
 * of the two values does not overflow, the result is the sum divided by two.
 */
public final class PercentileAverages {
    private PercentileAverages() {}

    public static double average(final int lo, final int hi) {
        return ((long) lo + hi) / 2.0;
    }

    public static double average(final long lo, final long hi) {
        final long sum = lo + hi;
        if (((lo ^ sum) & (hi ^ sum)) < 0) {
            // the sum overflowed; BigInteger rounds the exact sum to the nearest double, and halving it is exact
            return BigInteger.valueOf(lo).add(BigInteger.valueOf(hi)).doubleValue() / 2.0;
        }
        return sum / 2.0;
    }

    public static float average(final float lo, final float hi) {
        final float sum = lo + hi;
        if (Float.isInfinite(sum) && !Float.isInfinite(lo) && !Float.isInfinite(hi)) {
            return lo / 2 + hi / 2;
        }
        return sum / 2;
    }

    public static double average(final double lo, final double hi) {
        final double sum = lo + hi;
        if (Double.isInfinite(sum) && !Double.isInfinite(lo) && !Double.isInfinite(hi)) {
            return lo / 2 + hi / 2;
        }
        return sum / 2;
    }
}
