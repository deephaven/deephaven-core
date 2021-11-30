/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import io.deephaven.vector.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * A set of commonly used functions for casting between types.
 */
public class Casting {

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(byte[] values) {
        if (values == null) {
            return null;
        }
        long[] result = new long[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_BYTE) {
                result[i] = NULL_LONG;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(short[] values) {
        if (values == null) {
            return null;
        }
        long[] result = new long[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_SHORT) {
                result[i] = NULL_LONG;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(int[] values) {
        if (values == null) {
            return null;
        }
        long[] result = new long[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_INT) {
                result[i] = NULL_LONG;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(long[] values) {
        return values == null ? null : values.clone();
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(ByteVector values) {
        return values == null ? null : castLong(values.toArray());
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(ShortVector values) {
        return values == null ? null : castLong(values.toArray());
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(IntVector values) {
        return values == null ? null : castLong(values.toArray());
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(LongVector values) {
        return values == null ? null : castLong(values.toArray());
    }


    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(byte[] values) {
        if (values == null) {
            return null;
        }
        double[] result = new double[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_BYTE) {
                result[i] = NULL_DOUBLE;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(short[] values) {
        if (values == null) {
            return null;
        }
        double[] result = new double[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_SHORT) {
                result[i] = NULL_DOUBLE;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(int[] values) {
        if (values == null) {
            return null;
        }
        double[] result = new double[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_INT) {
                result[i] = NULL_DOUBLE;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(long[] values) {
        if (values == null) {
            return null;
        }
        double[] result = new double[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_LONG) {
                result[i] = NULL_DOUBLE;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(float[] values) {
        if (values == null) {
            return null;
        }
        double[] result = new double[values.length];
        for (int i = 0; i < result.length; i++) {
            if (values[i] == NULL_FLOAT) {
                result[i] = NULL_DOUBLE;
            } else {
                result[i] = values[i];
            }
        }
        return result;
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(double[] values) {
        return values == null ? null : values.clone();
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(ByteVector values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(ShortVector values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(IntVector values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(LongVector values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(FloatVector values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(DoubleVector values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an {@code int} array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    @Deprecated
    public static double[] intToDouble(int[] values) {
        return values == null ? null : castDouble(values);
    }

    /**
     * Casts an {@code long} array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    @Deprecated
    public static double[] longToDouble(long[] values) {
        return values == null ? null : castDouble(values);
    }

    /**
     * Casts an {@code int} array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    @Deprecated
    public static DoubleVector intToDouble(final IntVector values) {
        return values == null ? null : new DoubleVectorDirect(castDouble(values));
    }

    /**
     * Casts an {@code long} array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    @Deprecated
    public static DoubleVector longToDouble(final LongVector values) {
        return values == null ? null : new DoubleVectorDirect(castDouble(values));
    }

    static class LosingPrecisionWhileCastingException extends UnsupportedOperationException {

        private static final long serialVersionUID = 5998276378465685974L;

        public LosingPrecisionWhileCastingException(final String message) {
            super(message);
        }
    }
}
