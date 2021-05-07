/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

/**
 * A handful of simple mathematical utilities.
 */
public class MathUtil {

    /**
     * Compute ceil(log2(x)). See {@link Integer#numberOfLeadingZeros(int)}.
     *
     * @param x Input
     * @return ceil(log2(x))
     */
    public static int ceilLog2(int x) {
        return 32 - Integer.numberOfLeadingZeros(x - 1);
    }

    /**
     * Compute floor(log2(x)). See {@link Integer#numberOfLeadingZeros(int)}.
     *
     * @param x Input
     * @return floor(log2(x))
     */
    public static int floorLog2(int x) {
        return 31 - Integer.numberOfLeadingZeros(x);
    }

    /**
     * Compute ceil(log2(x)). See {@link Long#numberOfLeadingZeros(long)}.
     *
     * @param x Input
     * @return ceil(log2(x))
     */
    public static int ceilLog2(long x) {
        return 64 - Long.numberOfLeadingZeros(x - 1);
    }

    /**
     * Compute floor(log2(x)). See {@link Long#numberOfLeadingZeros(long)}.
     *
     * @param x Input
     * @return floor(log2(x))
     */
    public static int floorLog2(long x) {
        return 63 - Long.numberOfLeadingZeros(x);
    }

    /**
     * Compute the greatest common divisor of two integers using the Euclidean algorithm.
     *
     * @param a The first input
     * @param b The second input
     * @return The GCD
     * @implNote Always gives a non-negative result.
     */
    public static int gcd(int a, int b) {
        a = Math.abs(a);
        b = Math.abs(b);

        int t;
        while (b != 0) {
            t = b;
            b = a % b;
            a = t;
        }

        return a;
    }
}
