/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.db.tables.dbarrays.*;

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
    public static long[] castLong(DbByteArray values) {
        return values == null ? null : castLong(values.toArray());
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(DbShortArray values) {
        return values == null ? null : castLong(values.toArray());
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(DbIntArray values) {
        return values == null ? null : castLong(values.toArray());
    }

    /**
     * Casts an array to a {@code long} array.
     *
     * @param values values
     * @return cast array.
     */
    public static long[] castLong(DbLongArray values) {
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
    public static double[] castDouble(DbByteArray values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(DbShortArray values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(DbIntArray values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(DbLongArray values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(DbFloatArray values) {
        return values == null ? null : castDouble(values.toArray());
    }

    /**
     * Casts an array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    public static double[] castDouble(DbDoubleArray values) {
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
    public static DbDoubleArray intToDouble(final DbIntArray values) {
        return values == null ? null : new DbDoubleArrayDirect(castDouble(values));
    }

    /**
     * Casts an {@code long} array to a {@code double} array.
     *
     * @param values values
     * @return cast array.
     */
    @Deprecated
    public static DbDoubleArray longToDouble(final DbLongArray values) {
        return values == null ? null : new DbDoubleArrayDirect(castDouble(values));
    }

    static class LosingPrecisionWhileCastingException extends UnsupportedOperationException {

        private static final long serialVersionUID = 5998276378465685974L;

        public LosingPrecisionWhileCastingException(final String message) {
            super(message);
        }
    }
}
