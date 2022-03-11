package io.deephaven.util;

// See http://locklessinc.com/articles/sat_arithmetic/

public class SaturationArithmetic {
    public static long addSaturated(final long x, final long y) {
        final long res = x + y;
        if (res < x) {
            return Long.MAX_VALUE;
        }
        return res;
    }

    public static long minusSaturated(final long x, final long y) {
        final long res = x - y;
        if (res <= x) {
            return Long.MIN_VALUE;
        }
        return res;
    }

    // division saturated is same as normal.
    // multiplication saturated requires a 128 bit type so is a lot more complicated,
    // and we haven't had a need yet.
}
