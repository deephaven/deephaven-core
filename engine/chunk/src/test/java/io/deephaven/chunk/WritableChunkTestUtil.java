//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import java.util.Random;

final class WritableChunkTestUtil {

    private static final int FLOAT_NEG_ZERO_BITS = Float.floatToIntBits(-0.0f);
    private static final int FLOAT_POS_ZERO_BITS = Float.floatToIntBits(0.0f);

    private static final long DOUBLE_NEG_ZERO_BITS = Double.doubleToLongBits(-0.0);
    private static final long DOUBLE_POS_ZERO_BITS = Double.doubleToLongBits(0.0);

    public static boolean isPositiveZero(float value) {
        return Float.floatToIntBits(value) == FLOAT_POS_ZERO_BITS;
    }

    public static boolean isNegativeZero(float value) {
        return Float.floatToIntBits(value) == FLOAT_NEG_ZERO_BITS;
    }

    public static boolean isPositiveZero(double value) {
        return Double.doubleToLongBits(value) == DOUBLE_POS_ZERO_BITS;
    }

    public static boolean isNegativeZero(double value) {
        return Double.doubleToLongBits(value) == DOUBLE_NEG_ZERO_BITS;
    }

    public static float randomBitsFloat(Random r) {
        return Float.intBitsToFloat(r.nextInt());
    }

    public static double randomBitsDouble(Random r) {
        return Double.longBitsToDouble(r.nextLong());
    }

    public static float negativeZeroFloat() {
        return -0.0f;
    }

    public static float positiveZeroFloat() {
        return 0.0f;
    }

    public static double negativeZeroDouble() {
        return -0.0;
    }

    public static double positiveZeroDouble() {
        return 0.0;
    }
}
