package io.deephaven.utils;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Computes the square root of a BigDecimal using Babylonian method.
 */
public class BigDecimalUtils {
    private static final BigDecimal TWO = BigDecimal.valueOf(2);

    /**
     * Uses Babylonian method to determine the square root of a BigDecimal
     *
     * @param x the number to find the square root of
     * @param scale the desired scale for division and epsilon
     * @return the square root of x
     */
    public static BigDecimal sqrt(final BigDecimal x, final int scale) {
        if (x == null) {
            return null;
        }

        if (x.signum() == 0) {
            return BigDecimal.ZERO;
        }

        if (x.signum() < 0) {
            throw new IllegalArgumentException("x=" + x + " is negative.");
        }

        if (scale < 0) {
            throw new IllegalArgumentException("scale=" + scale + " is negative.");
        }

        final double intermediateInitial = Math.sqrt(x.doubleValue());
        final double initial = Double.isFinite(intermediateInitial) ? intermediateInitial : Math.sqrt(Double.MAX_VALUE);

        final BigDecimal epsilon = new BigDecimal(BigInteger.ONE, scale);

        BigDecimal x0 = BigDecimal.valueOf(initial);

        while (true) {
            final BigDecimal x1 =
                    x0.add(x.divide(x0, scale, BigDecimal.ROUND_HALF_UP)).divide(TWO, scale, BigDecimal.ROUND_HALF_UP);

            final BigDecimal difference = x1.subtract(x0).abs();
            x0 = x1;

            if (difference.compareTo(epsilon) <= 0) {
                break;
            }

        }
        return x0;
    }
}
