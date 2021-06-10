/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ShortNumericPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.dbarrays.*;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.*;

/**
 * A set of commonly used numeric functions that can be applied to Byte types.
 */
public class ByteNumericPrimitives {

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(Byte... values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (Byte c : values) {
            if (c != null && !BytePrimitives.isNull(c) && c > 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(byte[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return countPos(new DbByteArrayDirect(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(DbByteArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c) && c > 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(Byte... values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (Byte c : values) {
            if (c != null && !BytePrimitives.isNull(c) && c < 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(byte[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return countNeg(new DbByteArrayDirect(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(DbByteArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c) && c < 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(Byte... values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (Byte c : values) {
            if (c != null && !BytePrimitives.isNull(c) && c == 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(byte[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return countZero(new DbByteArrayDirect(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(DbByteArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c) && c == 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (Byte c : values) {
            if (c != null && !BytePrimitives.isNull(c)) {
                sum += c;
                count++;
            }
        }
        return sum / count;    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return avg(new DbByteArrayDirect(values));
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                sum += c;
                count++;
            }
        }
        return sum / count;
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (Byte c : values) {
            if (c != null && !BytePrimitives.isNull(c)) {
                sum += Math.abs(c);
                count++;
            }
        }
        return sum / count;
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return absAvg(new DbByteArrayDirect(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                sum += Math.abs(c);
                count++;
            }
        }
        return sum / count;
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (Byte c : values) {
            if (c != null && !BytePrimitives.isNull(c)) {
                double cc = (double) c;
                sum += cc;
                sum2 += cc * cc;
                count++;
            }
        }

        return sum2 / (count - 1) - sum * sum / count / (count - 1);
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return var(new DbByteArrayDirect(values));
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                double cc = (double) c;
                sum += cc;
                sum2 += cc * cc;
                count++;
            }
        }

        return sum2 / (count - 1) - sum * sum / count / (count - 1);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            byte c = values.get(i);
            double w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
                sum += w * c;
                sum2 += w * c * c;
                count += w;
            }
        }

        return sum2 / count - sum * sum / count / count;
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            byte c = values.get(i);
            float w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
                sum += w * c;
                sum2 += w * c * c;
                count += w;
            }
        }

        return sum2 / count - sum * sum / count / count;
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            byte c = values.get(i);
            byte w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !BytePrimitives.isNull(w)) {
                sum += w * c;
                sum2 += w * c * c;
                count += w;
            }
        }

        return sum2 / count - sum * sum / count / count;
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            byte c = values.get(i);
            int w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
                sum += w * c;
                sum2 += w * c * c;
                count += w;
            }
        }

        return sum2 / count - sum * sum / count / count;
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbByteArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            byte c = values.get(i);
            long w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
                sum += w * c;
                sum2 += w * c * c;
                count += w;
            }
        }

        return sum2 / count - sum * sum / count / count;
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double v = var(values);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return std(new DbByteArrayDirect(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double v = var(values);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double v = wvar(values, weights);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double v = wvar(values, weights);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double v = wvar(values, weights);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double v = wvar(values, weights);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbByteArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double v = wvar(values, weights);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final int c = BytePrimitives.count(values);
        return s == NULL_DOUBLE || c == NULL_INT ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return ste(new DbByteArrayDirect(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final int c = BytePrimitives.count(values);
        return s == NULL_DOUBLE || c == NULL_INT ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if(values.size() != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;

        for(int i=0; i<values.size(); i++) {
            final byte v = values.get(i);
            final double w = weights.get(i);

            if(!BytePrimitives.isNull(v) && !DoublePrimitives.isNull(w)){
                sumw += w;
                sumw2 += w*w;
            }
        }

        final double s = wstd(values, weights);
        return s == NULL_DOUBLE ? NULL_DOUBLE : s * Math.sqrt(sumw2/sumw/sumw);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if(values.size() != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;

        for(int i=0; i<values.size(); i++) {
            final byte v = values.get(i);
            final float w = weights.get(i);

            if(!BytePrimitives.isNull(v) && !FloatPrimitives.isNull(w)){
                sumw += w;
                sumw2 += w*w;
            }
        }

        final double s = wstd(values, weights);
        return s == NULL_DOUBLE ? NULL_DOUBLE : s * Math.sqrt(sumw2/sumw/sumw);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if(values.size() != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;

        for(int i=0; i<values.size(); i++) {
            final byte v = values.get(i);
            final byte w = weights.get(i);

            if(!BytePrimitives.isNull(v) && !BytePrimitives.isNull(w)){
                sumw += w;
                sumw2 += w*w;
            }
        }

        final double s = wstd(values, weights);
        return s == NULL_DOUBLE ? NULL_DOUBLE : s * Math.sqrt(sumw2/sumw/sumw);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if(values.size() != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;

        for(int i=0; i<values.size(); i++) {
            final byte v = values.get(i);
            final int w = weights.get(i);

            if(!BytePrimitives.isNull(v) && !IntegerPrimitives.isNull(w)){
                sumw += w;
                sumw2 += w*w;
            }
        }

        final double s = wstd(values, weights);
        return s == NULL_DOUBLE ? NULL_DOUBLE : s * Math.sqrt(sumw2/sumw/sumw);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbByteArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if(values.size() != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;

        for(int i=0; i<values.size(); i++) {
            final byte v = values.get(i);
            final long w = weights.get(i);

            if(!BytePrimitives.isNull(v) && !LongPrimitives.isNull(w)){
                sumw += w;
                sumw2 += w*w;
            }
        }

        final double s = wstd(values, weights);
        return s == NULL_DOUBLE ? NULL_DOUBLE : s * Math.sqrt(sumw2/sumw/sumw);
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double a = avg(values);
        final double s = ste(values);
        return a == NULL_DOUBLE || s == NULL_DOUBLE ? NULL_DOUBLE : avg(values) / ste(values);
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return tstat(new DbByteArrayDirect(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double a = avg(values);
        final double s = ste(values);
        return a == NULL_DOUBLE || s == NULL_DOUBLE ? NULL_DOUBLE : avg(values) / ste(values);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = weightedAvg(values, weights);
        final double s = wste(values, weights);
        return a / s;
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = weightedAvg(values, weights);
        final double s = wste(values, weights);
        return a / s;
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = weightedAvg(values, weights);
        final double s = wste(values, weights);
        return a / s;
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = weightedAvg(values, weights);
        final double s = wste(values, weights);
        return a / s;
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbByteArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = weightedAvg(values, weights);
        final double s = wste(values, weights);
        return a / s;
    }

    /**
     * Returns the maximum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    public static byte max(DbByteArray values) {
        if (values == null) {
            return NULL_BYTE;
        }

        byte val = NEG_INF_BYTE;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_BYTE : val;
    }

    /**
     * Returns the maximum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    public static byte max(byte[] values) {
        if (values == null) {
            return NULL_BYTE;
        }

        byte val = NEG_INF_BYTE;
        long count = 0;
        for (byte c : values) {
            if (!BytePrimitives.isNull(c)) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_BYTE : val;
    }

    /**
     * Returns the maximum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    public static byte max(Byte... values) {
        if (values == null) {
            return NULL_BYTE;
        }

        byte val = NEG_INF_BYTE;
        long count = 0;
        for (Byte c : values) {
            if (!(c == null || BytePrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_BYTE : val;
    }

    /**
     * Returns the minimum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static byte min(DbByteArray values) {
        if (values == null) {
            return NULL_BYTE;
        }

        byte val = POS_INF_BYTE;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_BYTE : val;
    }

    /**
     * Returns the minimum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static byte min(byte[] values) {
        if (values == null) {
            return NULL_BYTE;
        }

        byte val = POS_INF_BYTE;
        long count = 0;
        for (byte c : values) {
            if (!BytePrimitives.isNull(c)) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_BYTE : val;
    }

    /**
     * Returns the minimum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static byte min(Byte... values) {
        if (values == null) {
            return NULL_BYTE;
        }

        byte val = POS_INF_BYTE;
        long count = 0;
        for (Byte c : values) {
            if (!(c == null || BytePrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_BYTE : val;
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(Byte... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }
        int n = values.length;
        if (n == 0) {
            return Double.NaN;
        } else {
            byte[] copy = new byte[values.length];

            for(int i=0; i<values.length; i++) {
                copy[i] = values[i];
            }

            Arrays.sort(copy);
            if (n % 2 == 0)
                return 0.5 * (copy[n / 2 - 1] + copy[n / 2]);
            else return copy[n / 2];
        }
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(byte[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return median(new DbByteArrayDirect(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(DbByteArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }
        int n = values.intSize("median");
        if (n == 0) {
            return Double.NaN;
        } else {
            byte[] copy = new byte[(int)values.size()];

            for(int i=0; i<values.size(); i++) {
                copy[i] = values.get(i);
            }

            Arrays.sort(copy);
            if (n % 2 == 0)
                return 0.5 * (copy[n / 2 - 1] + copy[n / 2]);
            else return copy[n / 2];
        }
    }

    /**
     * Returns the percentile.
     *
     * @param percentile percentile to compute.
     * @param values values.
     * @return percentile.
     */
    public static double percentile(double percentile, byte... values) {
        if(values == null){
            return NULL_DOUBLE;
        }

        return percentile(percentile, new DbByteArrayDirect(values));
    }

    /**
     * Returns the percentile.
     *
     * @param percentile percentile to compute.
     * @param values values.
     * @return percentile.
     */
    public static double percentile(double percentile, DbByteArray values) {
        if(values == null){
            return NULL_DOUBLE;
        }

        if (percentile < 0 || percentile > 1)
            throw new RuntimeException("Invalid percentile = " + percentile);
        int n = values.intSize("percentile");

        byte[] copy = new byte[(int)values.size()];

        for(int i=0; i<values.size(); i++) {
            copy[i] = values.get(i);
        }

        Arrays.sort(copy);

        int idx = (int) Math.round(percentile * (n - 1));
        return copy[idx];
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static int firstIndexOf(byte[] values, byte val) {
        if (values == null) {
            return NULL_INT;
        }

        return firstIndexOf(new DbByteArrayDirect(values), val);
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static int firstIndexOf(DbByteArray values, byte val) {
        if (values == null) {
            return NULL_INT;
        }

        final long L = values.size();
        for (int i = 0; i < L; ++i) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c) && c == val) {
                return i;
            }
        }

        return NULL_INT;
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(Byte... values) {
        if (values == null) {
            return NULL_INT;
        }

        byte val = NEG_INF_BYTE;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.length; i++) {
            Byte c = values[i];
            if (c != null && !BytePrimitives.isNull(c) && c > val) {
                val = c;
                index = i;
                count++;
            }
        }

        return count == 0 ? -1 : index;
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(byte[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return indexOfMax(new DbByteArrayDirect(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(DbByteArray values) {
        if (values == null) {
            return NULL_INT;
        }

        byte val = NEG_INF_BYTE;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c) && c > val) {
                val = c;
                index = i;
                count++;
            }
        }

        return count == 0 ? -1 : index;
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(Byte... values) {
        if (values == null) {
            return NULL_INT;
        }

        byte val = POS_INF_BYTE;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.length; i++) {
            Byte c = values[i];
            if (c != null && !BytePrimitives.isNull(c) && c < val) {
                val = c;
                index = i;
                count++;
            }
        }

        return count == 0 ? -1 : index;
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(byte[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return indexOfMin(new DbByteArrayDirect(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(DbByteArray values) {
        if (values == null) {
            return NULL_INT;
        }

        byte val = POS_INF_BYTE;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c) && c < val) {
                val = c;
                index = i;
                count++;
            }
        }

        return count == 0 ? -1 : index;
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index immediately before where the key would be inserted.
     */
    public static int binSearchIndex(byte[] values, byte key, BinSearch choiceWhenEquals) {
        if (values == null) {
            return NULL_INT;
        }

        return binSearchIndex(new DbByteArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index immediately before where the key would be inserted.
     */
    public static int binSearchIndex(DbByteArray values, byte key, BinSearch choiceWhenEquals) {
        int index = rawBinSearchIndex(values, key, choiceWhenEquals);
        if (index == NULL_INT) {
            return index;
        }

        if (index < 0) {
            return -index - 2;
        } else {
            return index;
        }
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.
     * @param key key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(byte[] values, byte key, BinSearch choiceWhenEquals) {
        if (values == null) {
            return NULL_INT;
        }

        return rawBinSearchIndex(new DbByteArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.
     * @param key key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(DbByteArray values, byte key, BinSearch choiceWhenEquals) {
        if (values == null || key == NULL_BYTE) {
            return NULL_INT;
        }

        if (choiceWhenEquals != BinSearch.BS_ANY) {
            return binarySearch0Modified(values, 0, values.intSize("rawBinSearchIndex"), key, choiceWhenEquals == BinSearch.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, values.intSize("rawBinSearchIndex"), key);
        }
    }

    static private int binarySearch0(DbByteArray array, int fromIndex, int toIndex, byte key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            byte midVal = array.get(mid);
            if (midVal == NULL_BYTE) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    static private int binarySearch0Modified(DbByteArray array, int fromIndex, int toIndex, byte key, boolean highestOrLowest) {
        int low = fromIndex;
        int high = toIndex - 1;

        if (highestOrLowest) {
            if (high >= low && key == array.get(high)) {
                return high;
            }
        } else if (low <= high && key == array.get(low)) {
            return low;
        }

        while (low <= high) {
            int mid = highestOrLowest ? (low + high + 1) >>> 1 : (low + high) >>> 1;
            byte midVal = array.get(mid);
            if (midVal == NULL_BYTE) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            if (key > midVal) {
                low = mid + 1;
                if (low <= high) {
                    byte lowVal = array.get(low);
                    if (lowVal == NULL_BYTE) {
                        throw new RuntimeException("Can't have a null in the array!");
                    }
                    if (!highestOrLowest && key == lowVal) {
                        return low;
                    }
                }
            } else if (key < midVal) {
                high = mid - 1;
                if (high >= low) {
                    byte highVal = array.get(high);
                    if (highVal == NULL_BYTE) {
                        throw new RuntimeException("Can't have a null in the array!");
                    }
                    if (highestOrLowest && key == highVal) {
                        return high;
                    }
                }
            } else {
                if (highestOrLowest) {
                    low = mid;
                } else {
                    high = mid;
                }
            }
        }
        return -(low + 1);  // key not found.
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(byte[] values0, DbByteArray values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbByteArrayDirect(values0), values1);
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbByteArray values0, byte[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(values0, new DbByteArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(byte[] values0, byte[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbByteArrayDirect(values0), new DbByteArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbByteArray values0, DbByteArray values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        if (values0.size() != values1.size()) {
            throw new RuntimeException("Input arrays are different lengths!");
        }

        double sum0 = 0;
        double sum1 = 0;
        double sum01 = 0;
        double count = 0;

        for (int i = 0; i < values0.size(); i++) {
            if (!BytePrimitives.isNull(values0.get(i)) && !BytePrimitives.isNull(values1.get(i))) {
                sum0 += values0.get(i);
                sum1 += values1.get(i);
                sum01 += values0.get(i) * values1.get(i);
                count++;
            }
        }

        return sum01 / count - sum0 * sum1 / count / count;
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(byte[] values0, DbByteArray values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbByteArrayDirect(values0), values1);
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbByteArray values0, byte[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(values0, new DbByteArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(byte[] values0, byte[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbByteArrayDirect(values0), new DbByteArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbByteArray values0, DbByteArray values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        if (values0.size() != values1.size()) {
            throw new RuntimeException("Input arrays are different lengths!");
        }

        double sum0 = 0;
        double sum0Sq = 0;
        double sum1 = 0;
        double sum1Sq = 0;
        double sum01 = 0;
        double count = 0;

        for (int i = 0; i < values0.size(); i++) {
            if (!BytePrimitives.isNull(values0.get(i)) && !BytePrimitives.isNull(values1.get(i))) {
                sum0 += values0.get(i);
                sum0Sq += values0.get(i) * values0.get(i);
                sum1 += values1.get(i);
                sum1Sq += values1.get(i) * values1.get(i);
                sum01 += values0.get(i) * values1.get(i);
                count++;
            }
        }

        double cov = sum01 / count - sum0 * sum1 / count / count;
        double var0 = sum0Sq / count - sum0 * sum0 / count / count;
        double var1 = sum1Sq / count - sum1 * sum1 / count / count;

        return cov / Math.sqrt(var0 * var1);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static byte sum(DbByteArray values) {
        if (values == null) {
            return NULL_BYTE;
        }

        double sum = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (byte) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static byte sum(byte[] values) {
        if (values == null) {
            return NULL_BYTE;
        }

        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            byte c = values[i];
            if (!BytePrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (byte) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return array of sums of non-null values.
     */
    public static byte[] sum(DbArray<byte[]> values) {
        if (values == null || values.size() == 0) {
            return null;
        }

        byte[] result = new byte[values.get(0).length];

        for (int j = 0; j < values.size(); j++) {
            byte[] ai = values.get(j);
            Require.eq(ai.length, "a[i].length", result.length);

            for (int i = 0; i < ai.length; i++) {
                if (BytePrimitives.isNull(result[i]) || BytePrimitives.isNull(ai[i])) {
                    result[i] = NULL_BYTE;
                } else {
                    result[i] += ai[i];
                }
            }
        }

        return result;
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return array of sums of non-null values.
     */
    public static byte[] sum(byte[]... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        byte[] result = new byte[values[0].length];

        for (byte[] v : values) {
            Require.eq(v.length, "a[i].length", result.length);

            for (int i = 0; i < v.length; i++) {
                if (BytePrimitives.isNull(result[i]) || BytePrimitives.isNull(v[i])) {
                    result[i] = NULL_BYTE;
                } else {
                    result[i] += v[i];
                }
            }
        }

        return result;
    }

    /**
     * Returns the product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static byte product(DbByteArray values) {
        if (values == null) {
            return NULL_BYTE;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            byte c = values.get(i);
            if (!BytePrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_BYTE;
        }

        return (byte) (prod);
    }

    /**
     * Returns the product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static byte product(byte[] values) {
        if (values == null) {
            return NULL_BYTE;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.length; i++) {
            byte c = values[i];
            if (!BytePrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_BYTE;
        }

        return (byte) (prod);
    }

//    /**
//     * Returns the product.  Null values are excluded.  NaN values are included.
//     *
//     * @param values values.
//     * @return array of products of non-null values.
//     */
//    public static byte[] product(DbArray<byte[]> values) {
//        if (values == null || values.size() == 0) {
//            return null;
//        }
//
//        byte[] result = new byte[values.get(0).length];
//
//        for (int j = 0; j < values.size(); j++) {
//            byte[] ai = values.get(j);
//            Require.eq(ai.length, "a[i].length", result.length);
//
//            for (int i = 0; i < ai.length; i++) {
//                if (BytePrimitives.isNull(result[i]) || BytePrimitives.isNull(ai[i])) {
//                    result[i] = NULL_BYTE;
//                } else {
//                    result[i] *= ai[i];
//                }
//            }
//        }
//
//        return result;
//    }
//
//    /**
//     * Returns the product.  Null values are excluded.  NaN values are included.
//     *
//     * @param values values.
//     * @return array of products of non-null values.
//     */
//    public static byte[] product(byte[]... values) {
//        if (values == null || values.length == 0) {
//            return null;
//        }
//
//        byte[] result = new byte[values[0].length];
//
//        for (byte[] v : values) {
//            Require.eq(v.length, "a[i].length", result.length);
//
//            for (int i = 0; i < v.length; i++) {
//                if (BytePrimitives.isNull(result[i]) || BytePrimitives.isNull(v[i])) {
//                    result[i] = NULL_BYTE;
//                } else {
//                    result[i] *= v[i];
//                }
//            }
//        }
//
//        return result;
//    }

    /**
     * Returns the cumulative sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static byte[] cumsum(Byte... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0];
        }

        byte[] result = new byte[values.length];
        result[0] = values[0] == null ? NULL_BYTE : values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i-1] == NULL_BYTE || BytePrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (values[i] == null || BytePrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (byte) (result[i - 1] + values[i]);
            }
        }

        return result;
    }

    /**
     * Returns the cumulative sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static byte[] cumsum(byte[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0];
        }

        byte[] result = new byte[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (BytePrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (BytePrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (byte) (result[i - 1] + values[i]);
            }
        }

        return result;
    }

    /**
     * Returns the cumulative sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static byte[] cumsum(DbByteArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new byte[0];
        }

        byte[] result = new byte[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (BytePrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (BytePrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (byte) (result[i - 1] + values.get(i));
            }
        }

        return result;
    }

    /**
     * Returns the cumulative product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static byte[] cumprod(Byte... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0];
        }

        byte[] result = new byte[values.length];
        result[0] = values[0] == null ? NULL_BYTE : values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i-1] == NULL_BYTE || BytePrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (values[i] == null || BytePrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (byte) (result[i - 1] * values[i]);
            }
        }

        return result;
    }

    /**
     * Returns the cumulative product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static byte[] cumprod(byte[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new byte[0];
        }

        byte[] result = new byte[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (BytePrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (BytePrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (byte) (result[i - 1] * values[i]);
            }
        }

        return result;
    }

    /**
     * Returns the cumulative product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static byte[] cumprod(DbByteArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new byte[0];
        }

        byte[] result = new byte[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (BytePrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (BytePrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (byte) (result[i - 1] * values.get(i));
            }
        }

        return result;
    }

    /**
     * Returns the absolute value.
     *
     * @param value value.
     * @return absolute value.
     */
    public static byte abs(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_BYTE;
        }

        return (byte) Math.abs(value);
    }

    /**
     * Returns the arc cosine.
     *
     * @param value value.
     * @return arc cosine.
     */
    public static double acos(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.acos(value);
    }

    /**
     * Returns the arc sine.
     *
     * @param value value.
     * @return arc sine.
     */
    public static double asin(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.asin(value);
    }

    /**
     * Returns the arc tangent.
     *
     * @param value value.
     * @return arc tangent.
     */
    public static double atan(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.atan(value);
    }

    /**
     * Returns the ceiling.  This is the smallest integer, which is greater than or equal to the value.
     *
     * @param value value.
     * @return ceiling.
     */
    public static double ceil(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.ceil(value);
    }

    /**
     * Returns the cosine.
     *
     * @param value value.
     * @return cosine.
     */
    public static double cos(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.cos(value);
    }

    /**
     * Returns Euler's number <i>e</i> raised to a power.
     *
     * @param value value.
     * @return Euler's number <i>e</i> raised to a power.
     */
    public static double exp(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.exp(value);
    }

    /**
     * Returns the floor.  This is the largest integer, which is less than or equal to the value.
     *
     * @param value value.
     * @return floor.
     */
    public static double floor(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.floor(value);
    }

    /**
     * Returns the natural logarithm (base <i>e</i>).
     *
     * @param value value.
     * @return natural logarithm (base <i>e</i>).
     */
    public static double log(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.log(value);
    }

    /**
     * Returns the value of the first argument raised to the second argument.
     *
     * @param   a   the base.
     * @param   b   the exponent.
     * @return {@code a} raised to the {@code b} power.
     */
    public static double pow(byte a, byte b) {
        if (BytePrimitives.isNull(a) || BytePrimitives.isNull(b)) {
            return NULL_DOUBLE;
        }

        return Math.pow(a, b);
    }

    /**
     * Returns the integer closest to the input value.
     *
     * @param value value.
     * @return integer closes to the input value.
     */
    public static double rint(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.rint(value);
    }

    /**
     * Returns the closest integer to the argument.  If the argument is NaN, the result is 0.  If the argument is greater
     * than {@code Integer.MIN_VALUE}, {@code Integer.MIN_VALUE} is returned.  If the argument is less than {@code Integer.MAX_VALUE},
     * {@code Integer.MAX_VALUE} is returned.
     *
     * @param value value.
     */
    public static long round(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_LONG;
        }

        return Math.round(value);
    }

    /**
     * Returns the signum function.
     *
     * @param value value.
     * @return signum function.
     */
    public static byte signum(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_BYTE;
        }

        return (byte) Integer.signum(value);
    }

    /**
     * Returns the sine.
     *
     * @param value value.
     * @return sine.
     */
    public static double sin(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.sin(value);
    }

    /**
     * Returns the square root.
     *
     * @param value value.
     * @return square root.
     */
    public static double sqrt(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.sqrt(value);
    }

    /**
     * Returns the tangent.
     *
     * @param value value.
     * @return tangent.
     */
    public static double tan(byte value) {
        if (BytePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.tan(value);
    }

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @return lower bound of the bin containing the value.
     */
    public static byte lowerBin(byte value, byte interval) {
        if (value == NULL_BYTE || interval == NULL_BYTE) {
            return NULL_BYTE;
        }

        return (byte) (interval * ((byte) Math.floor(value / interval)));
    }

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return lower bound of the bin containing the value.
     */
    public static byte lowerBin(byte value, byte interval, byte offset) {
        if (value == NULL_BYTE || interval == NULL_BYTE) {
            return NULL_BYTE;
        }

        return (byte)(lowerBin((byte)(value-offset),interval) + offset);
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @return upper bound of the bin containing the value.
     */
    public static byte upperBin(byte value, byte interval) {
        if (value == NULL_BYTE || interval == NULL_BYTE) {
            return NULL_BYTE;
        }

        final double r = ((double) value) / ((double) interval);

        if (r == Math.round(r)) {
            return (byte) (interval * r);
        }

        return (byte) (interval * ((byte) Math.floor(r + 1)));
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return upper bound of the bin containing the value.
     */
    public static byte upperBin(byte value, byte interval, byte offset) {
        if (value == NULL_BYTE || interval == NULL_BYTE) {
            return NULL_BYTE;
        }

        return (byte)(upperBin((byte)(value-offset),interval) + offset);
    }

    /**
     * Constrains the value to be on the {@code [min,max]} range.  If the value is less than {@code min}, {@code min} is returned.
     * If the value is greater than {@code max}, {@code max} is returned.
     *
     * @param value value.
     * @param min minimum value.
     * @param max maximum value.
     * @return value constrained to be in the {@code [min,max]} range.
     */
    public static byte clamp(byte value, byte min, byte max) {
        Require.leq(min, "min", max, "max");

        if (BytePrimitives.isNull(value)) {
            return NULL_BYTE;
        }

        if (value < min) {
            return min;
        } else if (value > max) {
            return max;
        } else {
            return value;
        }
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, DbByteArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            byte w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !BytePrimitives.isNull(w)) {
                vsum += c * w;
            }
        }
        return vsum;
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, DbLongArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            long w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
                vsum += c * w;
            }
        }
        return vsum;
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, DbDoubleArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            double w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
                vsum += c * w;
            }
        }
        return vsum;
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, DbFloatArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            float w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
                vsum += c * w;
            }
        }
        return vsum;
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbByteArray values, DbIntArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbByteArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            int w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
                vsum += c * w;
            }
        }
        return vsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, DbDoubleArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            double w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, DbFloatArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            float w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, DbLongArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        long wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            long w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, DbIntArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        int wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            int w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbByteArray values, DbByteArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(byte[] values, byte[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbByteArrayDirect(values), new DbByteArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbByteArray values, DbByteArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        int wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            byte c = values.get(i);
            byte w = weights.get(i);
            if (!BytePrimitives.isNull(c) && !BytePrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static DbByteArray sort(final DbByteArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbByteArrayDirect();
        }

        final byte[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        return new DbByteArrayDirect(vs);
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static byte[] sort(final byte[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new byte[]{};
        }

        final byte[] vs = Arrays.copyOf(values, values.length);
        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static byte[] sort(final Byte... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new byte[]{};
        }

        final byte[] vs = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_BYTE : values[i];
        }
        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static DbByteArray sortDescending(final DbByteArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbByteArrayDirect();
        }

        final byte[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return new DbByteArrayDirect(vs);
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static byte[] sortDescending(final byte[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new byte[]{};
        }

        final byte[] vs = Arrays.copyOf(values, values.length);
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return vs;
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static byte[] sortDescending(final Byte... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new byte[]{};
        }

        final byte[] vs = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_BYTE : values[i];
        }
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return vs;
    }

    /**
     * Returns a sequence of values.
     *
     * @param start starting value.
     * @param end terminal value.
     * @param step step size.
     * @return sequence of values from start to end.
     */
    public static byte[] sequence(byte start, byte end, byte step) {
        if(step == 0) {
            return new byte[0];
        }

        final int n = (int)((end-start)/step);

        if(n < 0) {
            return new byte[0];
        }

        final byte[] result = new byte[n+1];

        for(int i=0; i<=n; i++){
            result[i] = (byte)(start + i*step);
        }

        return result;
    }

}
