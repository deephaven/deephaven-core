/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base;

public class ClampUtil {
    /**
     * @param min
     * @param max
     * @param x
     * @return min, if x&lt;min; max, if x&gt;max; else x
     */
    public static double clamp(final double min, final double max, final double x) {
        if (Double.isNaN(min) || Double.isNaN(max)) {
            return Double.NaN;
        }
        if (x < min) {
            return min;
        } else if (x > max) {
            return max;
        }
        return x;
    }

    /**
     * @param min
     * @param max
     * @param x
     * @return min, if x&lt;min; max, if x&gt;max; else x
     */
    public static int clampInt(final int min, final int max, final int x) {
        if (x < min) {
            return min;
        } else if (x > max) {
            return max;
        }
        return x;
    }

    /**
     * @param min
     * @param max
     * @param x
     * @return min, if x&lt;min; max, if x&gt;max; else x
     */
    public static long clampLong(final long min, final long max, final long x) {
        if (x < min) {
            return min;
        } else if (x > max) {
            return max;
        }
        return x;
    }
}
