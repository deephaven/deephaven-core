//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

    private static final int[] tenToThe = new int[] {
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
    };

    /**
     * Compute 10^n as a int for 0 &lt;= n &lt;= 9.
     *
     * @param n the exponent
     * @return 10^n
     */
    public static int pow10(int n) {
        if (n < 0 || n > 9) {
            throw new IllegalArgumentException("n = " + n);
        }
        return tenToThe[n];
    }

    private static final int[] base10guessFromBase2Digits = new int[] {
            0, 0, 0, 0, 1, 1, 1, 2, 2, 2,
            3, 3, 3, 3, 4, 4, 4, 5, 5, 5,
            6, 6, 6, 6, 7, 7, 7, 8, 8, 8,
            9, 9, 9
    };

    /**
     * Compute the number of base 10 digits in n's representation, for n &gt;= 0.
     * 
     * @param n an integer &gt;= 0
     * @return how many digits in n's base 10 representation.
     */
    public static int base10digits(int n) {
        int baseTwoDigits = 32 - Integer.numberOfLeadingZeros(n);
        int base10guess = base10guessFromBase2Digits[baseTwoDigits];
        if (n >= pow10(base10guess)) {
            return 1 + base10guess;
        }
        return base10guess;
    }
}
