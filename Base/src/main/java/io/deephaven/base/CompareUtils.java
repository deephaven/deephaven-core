/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

public class CompareUtils {
    public static final double ERROR = 0.000001;

    public static int compare(Object data1, Object data2) {
        if (data1 == null ^ data2 == null) {
            return data1 == null ? -1 : 1;
        }

        if (data1 != null && data2 != null) {
            return ((Comparable) data1).compareTo(data2);
        }

        return 0;
    }

    public static int compare(long a, long b) {
        if (a > b) {
            return 1;
        } else if (a < b) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int compareUnsigned(long a, long b) {
        if (a < 0 && b >= 0) {
            return 1;
        }
        if (a >= 0 && b < 0) {
            return -1;
        }
        return compare(a, b);
    }

    public static int compare(double a, double b) {
        if (a > b) {
            return 1;
        } else if (a < b) {
            return -1;
        } else {
            return 0;
        }
    }

    public static boolean equals(Object data1, Object data2) {
        if (data1 == null) {
            return data2 == null;
        }
        if (data2 == null) {
            return false;
        }
        return data1 == data2 || data1.equals(data2);
    }

    /**
     * Returns true if the given objects are both null, or equal by the first object's
     * {@link #equals} method.
     */
    public static boolean nullSafeEquals(Object left, Object right) {
        if (null == left) {
            return null == right;
        } else {
            return left.equals(right);
        }
    }

    /**
     * You can't do double1 == double2 because floating point numbers are not exact values. Just
     * make sure that x-y is less than some allowable error factor.
     *
     * @param x
     * @param y
     * @return True if the two doubles are equal to each other (or so close that we don't care that
     *         they are different).
     */
    public static boolean EQ(double x, double y) {
        return doubleEquals(x, y);
    } // I'm a lazy typist.

    public static boolean doubleEquals(double x, double y) {
        return doubleEquals(x, y, ERROR);
    }

    public static boolean doubleEqualsZero(double x) {
        return doubleEqualsZero(x, ERROR);
    }

    /**
     * You can't do double1 == double2 because floating point numbers are not exact values. Just
     * make sure that x-y is less than some allowable error factor.
     *
     * @param x
     * @param y
     * @return True if the two doubles are equal to each other (or so close that we don't care that
     *         they are different). Also true if both are NaN.
     */
    public static boolean doubleEquals(double x, double y, double tolerance) {
        return (Double.isNaN(x) && Double.isNaN(y)) || (Math.abs(x - y) < tolerance);
    }

    public static boolean doubleEqualsZero(double x, double tolerance) {
        return (Math.abs(x) < tolerance);
    }

    public static boolean EQ2(double x, double y) {
        return doubleEquals2(x, y, ERROR);
    }

    public static boolean doubleEquals2(double x, double y) {
        return doubleEquals2(x, y, ERROR);
    }

    public static boolean doubleEquals2(double x, double y, double tolerance) {
        return Double.compare(x, y) == 0 ||
            Math.abs(x - y) < tolerance;
    }

    /**
     * Since logical comparison of double values considerig error is effectively a three-value
     * logic, you can't really do !equals when you mean notEquals.
     * 
     * @param x
     * @param y
     * @return True if two doubles are apart from each other enough that we consider them different.
     *         False if both of them are NaN
     */
    public static boolean NE(double x, double y) {
        return doubleNotEquals(x, y);
    }

    public static boolean doubleNotEquals(double x, double y) {
        return doubleNotEquals(x, y, ERROR);
    }

    public static boolean doubleNotEquals(double x, double y, double tolerance) {
        final boolean isNaNx = Double.isNaN(x);
        final boolean isNaNy = Double.isNaN(y);
        if (!isNaNx && !isNaNy) {
            return (Math.abs(x - y) > tolerance);
        }
        return isNaNx ^ isNaNy;
    }

    public static boolean NE2(double x, double y) {
        return !doubleEquals2(x, y, ERROR);
    }

    public static boolean doubleNotEquals2(double x, double y) {
        return !doubleEquals2(x, y, ERROR);
    }

    public static boolean doubleNotEquals2(double x, double y, double tolerance) {
        return Double.compare(x, y) != 0 &&
            !(Math.abs(x - y) < tolerance);
    }

    /**
     * You can't do double1 > double2 because floating point numbers are not exact values. Just make
     * sure that x-y is greater than some allowable error factor for equality
     *
     * @param x
     * @param y
     * @return True if x is greater than y (including error factor for equality).
     */
    public static boolean GT(double x, double y) {
        return doubleGreater(x, y);
    } // I'm a lazy typist.

    public static boolean doubleGreater(double x, double y) {
        return x - y > ERROR;
    }

    public static boolean doubleGreater(double x, double y, double tolerance) {
        return x - y > tolerance;
    }

    public static boolean GE(double x, double y) {
        return doubleGreaterEqual(x, y);
    } // I'm a lazy typist.

    public static boolean doubleGreaterEqual(double x, double y) {
        return y - x < ERROR;
    }

    public static boolean doubleGreaterEqual(double x, double y, double tolerance) {
        return y - x < tolerance;
    }

    /**
     * You can't do double1 < double2 because floating point numbers are not exact values. Just make
     * sure that y - x is greater than some allowable error factor for equality
     *
     * @param x
     * @param y
     * @return True if x is less than y (including error factor for equality)
     */
    public static boolean doubleLess(double x, double y, double tolerance) {
        return y - x > tolerance;
    }

    public static boolean LT(double x, double y) {
        return doubleLess(x, y);
    } // I'm a lazy typist.

    public static boolean doubleLess(double x, double y) {
        return y - x > ERROR;
    }

    public static boolean doubleLessEqual(double x, double y, double tolerance) {
        return x - y < tolerance;
    }

    public static boolean LE(double x, double y) {
        return doubleLessEqual(x, y);
    } // I'm a lazy typist.

    public static boolean doubleLessEqual(double x, double y) {
        return x - y < ERROR;
    }

    public static int doubleCompare(double d1, double d2) {
        if (doubleEquals(d1, d2)) {
            return 0;
        }

        return Double.compare(d1, d2);
    }
}
