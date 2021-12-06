/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.function;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * A set of commonly used functions for comparing primitive pairs.
 */
public class ComparePrimitives {
    ///////////// byte //////////////////

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static byte max(final byte v1, final byte v2) {
        return BytePrimitives.isNull(v1) ? v2 : BytePrimitives.isNull(v2) ? v1 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static short max(final byte v1, final short v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int max(final byte v1, final int v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final byte v1, final long v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float max(final byte v1, final float v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final byte v1, final double v2) {
        return max(v2, v1);
    }


    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static byte min(final byte v1, final byte v2) {
        return BytePrimitives.isNull(v1) ? v2 : BytePrimitives.isNull(v2) ? v1 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static short min(final byte v1, final short v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int min(final byte v1, final int v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final byte v1, final long v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float min(final byte v1, final float v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final byte v1, final double v2) {
        return min(v2, v1);
    }


    ///////////// short //////////////////

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static short max(final short v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? v1 : ShortPrimitives.isNull(v1) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static short max(final short v1, final short v2) {
        return ShortPrimitives.isNull(v1) ? v2 : ShortPrimitives.isNull(v2) ? v1 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int max(final short v1, final int v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final short v1, final long v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float max(final short v1, final float v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final short v1, final double v2) {
        return max(v2, v1);
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static short min(final short v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? v1 : ShortPrimitives.isNull(v1) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static short min(final short v1, final short v2) {
        return ShortPrimitives.isNull(v1) ? v2 : ShortPrimitives.isNull(v2) ? v1 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int min(final short v1, final int v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final short v1, final long v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float min(final short v1, final float v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final short v1, final double v2) {
        return min(v2, v1);
    }


    /////////////// int ///////////////////

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int max(final int v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? v1 : IntegerPrimitives.isNull(v1) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int max(final int v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? v1 : IntegerPrimitives.isNull(v1) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int max(final int v1, final int v2) {
        return IntegerPrimitives.isNull(v1) ? v2 : IntegerPrimitives.isNull(v2) ? v1 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final int v1, final long v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final int v1, final float v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final int v1, final double v2) {
        return max(v2, v1);
    }


    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int min(final int v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? v1 : IntegerPrimitives.isNull(v1) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int min(final int v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? v1 : IntegerPrimitives.isNull(v1) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static int min(final int v1, final int v2) {
        return IntegerPrimitives.isNull(v1) ? v2 : IntegerPrimitives.isNull(v2) ? v1 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final int v1, final long v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final int v1, final float v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final int v1, final double v2) {
        return min(v2, v1);
    }


    ///////////////// long ////////////////

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final long v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? v1 : LongPrimitives.isNull(v1) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final long v1, final long v2) {
        return LongPrimitives.isNull(v1) ? v2 : LongPrimitives.isNull(v2) ? v1 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final long v1, final int v2) {
        return IntegerPrimitives.isNull(v2) ? v1 : LongPrimitives.isNull(v1) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long max(final long v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? v1 : LongPrimitives.isNull(v1) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final long v1, final float v2) {
        return max(v2, v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final long v1, final double v2) {
        return max(v2, v1);
    }


    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final long v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? v1 : LongPrimitives.isNull(v1) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final long v1, final long v2) {
        return LongPrimitives.isNull(v1) ? v2 : LongPrimitives.isNull(v2) ? v1 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final long v1, final int v2) {
        return IntegerPrimitives.isNull(v2) ? v1 : LongPrimitives.isNull(v1) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static long min(final long v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? v1 : LongPrimitives.isNull(v1) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final long v1, final float v2) {
        return min(v2, v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final long v1, final double v2) {
        return min(v2, v1);
    }


    ///////////////// float ////////////////

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float max(final float v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? (Float.isNaN(v1) ? NULL_FLOAT : v1)
                : (Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float max(final float v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? (Float.isNaN(v1) ? NULL_FLOAT : v1)
                : (Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final float v1, final int v2) {
        return max((Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? NULL_DOUBLE : (double) v1, v2);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final float v1, final long v2) {
        return max((Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? NULL_DOUBLE : (double) v1, v2);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float max(final float v1, final float v2) {
        final boolean isV1NaN = Float.isNaN(v1);
        final boolean isV1Null = FloatPrimitives.isNull(v1);
        final boolean isV1Invalid = isV1NaN || isV1Null;
        final boolean isV2NaN = Float.isNaN(v2);
        final boolean isV2Null = FloatPrimitives.isNull(v2);
        final boolean isV2Invalid = isV2NaN || isV2Null;

        return isV1Invalid ? (isV2Invalid ? NULL_FLOAT : v2) : (isV2Invalid ? v1 : v1 < v2 ? v2 : v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final float v1, final double v2) {
        return max(v2, v1);
    }


    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float min(final float v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? (Float.isNaN(v1) ? NULL_FLOAT : v1)
                : (Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float min(final float v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? (Float.isNaN(v1) ? NULL_FLOAT : v1)
                : (Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final float v1, final int v2) {
        return min((Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? NULL_DOUBLE : (double) v1, v2);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final float v1, final long v2) {
        return min((Float.isNaN(v1) || FloatPrimitives.isNull(v1)) ? NULL_DOUBLE : (double) v1, v2);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static float min(final float v1, final float v2) {
        final boolean isV1NaN = Float.isNaN(v1);
        final boolean isV1Null = FloatPrimitives.isNull(v1);
        final boolean isV1Invalid = isV1NaN || isV1Null;
        final boolean isV2NaN = Float.isNaN(v2);
        final boolean isV2Null = FloatPrimitives.isNull(v2);
        final boolean isV2Invalid = isV2NaN || isV2Null;

        return isV1Invalid ? (isV2Invalid ? NULL_FLOAT : v2) : (isV2Invalid ? v1 : v1 > v2 ? v2 : v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final float v1, final double v2) {
        return min(v2, v1);
    }


    ///////////// double /////////////

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final double v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? (Double.isNaN(v1) ? NULL_DOUBLE : v1)
                : (Double.isNaN(v1) || DoublePrimitives.isNull(v1)) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final double v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? (Double.isNaN(v1) ? NULL_DOUBLE : v1)
                : (Double.isNaN(v1) || DoublePrimitives.isNull(v1)) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final double v1, final int v2) {
        return IntegerPrimitives.isNull(v2) ? (Double.isNaN(v1) ? NULL_DOUBLE : v1)
                : (Double.isNaN(v1) || DoublePrimitives.isNull(v1)) ? v2 : v1 < v2 ? v2 : v1;
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final double v1, final long v2) {
        if (LongPrimitives.isNull(v2)) {
            return Double.isNaN(v1) ? NULL_DOUBLE : v1;
        } else if ((Double.isNaN(v1) || DoublePrimitives.isNull(v1))) {
            return returnValue(v2);
        } else {
            final int compare = Comparators.compare(v1, v2);
            if (compare < 0) {
                try {
                    return returnValue(v2);
                } catch (final Casting.LosingPrecisionWhileCastingException uoe) {
                    throw new Casting.LosingPrecisionWhileCastingException("Not supported: max(" + v1 + ", " + v2
                            + "), because the result loses the precision while being cast to double.");
                }
            } else {
                return v1;
            }
        }
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final double v1, final float v2) {
        final boolean isV1NaN = Double.isNaN(v1);
        final boolean isV1Null = DoublePrimitives.isNull(v1);
        final boolean isV1Invalid = isV1NaN || isV1Null;
        final boolean isV2NaN = Float.isNaN(v2);
        final boolean isV2Null = FloatPrimitives.isNull(v2);
        final boolean isV2Invalid = isV2NaN || isV2Null;

        return isV1Invalid ? (isV2Invalid ? NULL_DOUBLE : v2) : (isV2Invalid ? v1 : v1 < v2 ? v2 : v1);
    }

    /**
     * Returns the maximum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return maximum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double max(final double v1, final double v2) {
        final boolean isV1NaN = Double.isNaN(v1);
        final boolean isV1Null = DoublePrimitives.isNull(v1);
        final boolean isV1Invalid = isV1NaN || isV1Null;
        final boolean isV2NaN = Double.isNaN(v2);
        final boolean isV2Null = DoublePrimitives.isNull(v2);
        final boolean isV2Invalid = isV2NaN || isV2Null;

        return isV1Invalid ? (isV2Invalid ? NULL_DOUBLE : v2) : (isV2Invalid ? v1 : v1 < v2 ? v2 : v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final double v1, final byte v2) {
        return BytePrimitives.isNull(v2) ? (Double.isNaN(v1) ? NULL_DOUBLE : v1)
                : (Double.isNaN(v1) || DoublePrimitives.isNull(v1)) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final double v1, final short v2) {
        return ShortPrimitives.isNull(v2) ? (Double.isNaN(v1) ? NULL_DOUBLE : v1)
                : (Double.isNaN(v1) || DoublePrimitives.isNull(v1)) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final double v1, final int v2) {
        return IntegerPrimitives.isNull(v2) ? (Double.isNaN(v1) ? NULL_DOUBLE : v1)
                : (Double.isNaN(v1) || DoublePrimitives.isNull(v1)) ? v2 : v1 > v2 ? v2 : v1;
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final double v1, final long v2) {
        if (LongPrimitives.isNull(v2)) {
            return Double.isNaN(v1) ? NULL_DOUBLE : v1;
        } else if ((Double.isNaN(v1) || DoublePrimitives.isNull(v1))) {
            return returnValue(v2);
        } else {
            final int compare = Comparators.compare(v1, v2);
            if (compare > 0) {
                try {
                    return returnValue(v2);
                } catch (final Casting.LosingPrecisionWhileCastingException uoe) {
                    throw new Casting.LosingPrecisionWhileCastingException("Not supported: min(" + v1 + ", " + v2
                            + "), because the result loses the precision while being cast to double.");
                }

            } else {
                return v1;
            }
        }
    }

    private static boolean isLosingPrecision(final long v) {
        return v != (long) ((double) (v));
    }

    private static double returnValue(final long v) {
        if (isLosingPrecision(v)) {
            // throw error
            throw new Casting.LosingPrecisionWhileCastingException(
                    "Not supported because the value, " + v + ", loses the precision while being cast to double.");
        } else {
            return v;
        }
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final double v1, final double v2) {
        final boolean isV1NaN = Double.isNaN(v1);
        final boolean isV1Null = DoublePrimitives.isNull(v1);
        final boolean isV1Invalid = isV1NaN || isV1Null;
        final boolean isV2NaN = Double.isNaN(v2);
        final boolean isV2Null = DoublePrimitives.isNull(v2);
        final boolean isV2Invalid = isV2NaN || isV2Null;

        return isV1Invalid ? (isV2Invalid ? NULL_DOUBLE : v2) : (isV2Invalid ? v1 : v1 > v2 ? v2 : v1);
    }

    /**
     * Returns the minimum. Null and NaN values are excluded.
     *
     * @param v1 first value.
     * @param v2 second value.
     * @return minimum of the valid input values. If both inputs are invalid, null is returned.
     */
    public static double min(final double v1, final float v2) {
        final boolean isV1NaN = Double.isNaN(v1);
        final boolean isV1Null = DoublePrimitives.isNull(v1);
        final boolean isV1Invalid = isV1NaN || isV1Null;
        final boolean isV2NaN = Float.isNaN(v2);
        final boolean isV2Null = FloatPrimitives.isNull(v2);
        final boolean isV2Invalid = isV2NaN || isV2Null;

        return isV1Invalid ? (isV2Invalid ? NULL_DOUBLE : v2) : (isV2Invalid ? v1 : v1 > v2 ? v2 : v1);
    }
}
