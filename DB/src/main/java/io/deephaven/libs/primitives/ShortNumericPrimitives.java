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
 * A set of commonly used numeric functions that can be applied to Short types.
 */
public class ShortNumericPrimitives {

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(Short... values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (Short c : values) {
            if (c != null && !ShortPrimitives.isNull(c) && c > 0) {
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
    public static int countPos(short[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return countPos(new DbShortArrayDirect(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(DbShortArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c) && c > 0) {
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
    public static int countNeg(Short... values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (Short c : values) {
            if (c != null && !ShortPrimitives.isNull(c) && c < 0) {
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
    public static int countNeg(short[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return countNeg(new DbShortArrayDirect(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(DbShortArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c) && c < 0) {
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
    public static int countZero(Short... values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (Short c : values) {
            if (c != null && !ShortPrimitives.isNull(c) && c == 0) {
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
    public static int countZero(short[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return countZero(new DbShortArrayDirect(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(DbShortArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c) && c == 0) {
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
    public static double avg(Short... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (Short c : values) {
            if (c != null && !ShortPrimitives.isNull(c)) {
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
    public static double avg(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return avg(new DbShortArrayDirect(values));
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(DbShortArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
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
    public static double absAvg(Short... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (Short c : values) {
            if (c != null && !ShortPrimitives.isNull(c)) {
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
    public static double absAvg(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return absAvg(new DbShortArrayDirect(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(DbShortArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
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
    public static double var(Short... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (Short c : values) {
            if (c != null && !ShortPrimitives.isNull(c)) {
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
    public static double var(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return var(new DbShortArrayDirect(values));
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(DbShortArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
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
    public static double wvar(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, double[] weights) {
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
    public static double wvar(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            short c = values.get(i);
            double w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wvar(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, float[] weights) {
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
    public static double wvar(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            short c = values.get(i);
            float w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wvar(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            short c = values.get(i);
            short w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static double wvar(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, int[] weights) {
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
    public static double wvar(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            short c = values.get(i);
            int w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wvar(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, long[] weights) {
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
    public static double wvar(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbShortArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            short c = values.get(i);
            long w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double std(Short... values) {
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
    public static double std(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return std(new DbShortArrayDirect(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(DbShortArray values) {
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
    public static double wstd(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, double[] weights) {
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
    public static double wstd(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, DbDoubleArray weights) {
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
    public static double wstd(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, float[] weights) {
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
    public static double wstd(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, DbFloatArray weights) {
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
    public static double wstd(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, DbShortArray weights) {
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
    public static double wstd(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, int[] weights) {
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
    public static double wstd(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, DbIntArray weights) {
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
    public static double wstd(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, long[] weights) {
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
    public static double wstd(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbShortArray values, DbLongArray weights) {
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
    public static double ste(Short... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final int c = ShortPrimitives.count(values);
        return s == NULL_DOUBLE || c == NULL_INT ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return ste(new DbShortArrayDirect(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(DbShortArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final int c = ShortPrimitives.count(values);
        return s == NULL_DOUBLE || c == NULL_INT ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, double[] weights) {
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
    public static double wste(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, DbDoubleArray weights) {
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
            final short v = values.get(i);
            final double w = weights.get(i);

            if(!ShortPrimitives.isNull(v) && !DoublePrimitives.isNull(w)){
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
    public static double wste(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, float[] weights) {
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
    public static double wste(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, DbFloatArray weights) {
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
            final short v = values.get(i);
            final float w = weights.get(i);

            if(!ShortPrimitives.isNull(v) && !FloatPrimitives.isNull(w)){
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
    public static double wste(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, DbShortArray weights) {
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
            final short v = values.get(i);
            final short w = weights.get(i);

            if(!ShortPrimitives.isNull(v) && !ShortPrimitives.isNull(w)){
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
    public static double wste(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, int[] weights) {
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
    public static double wste(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, DbIntArray weights) {
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
            final short v = values.get(i);
            final int w = weights.get(i);

            if(!ShortPrimitives.isNull(v) && !IntegerPrimitives.isNull(w)){
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
    public static double wste(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, long[] weights) {
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
    public static double wste(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbShortArray values, DbLongArray weights) {
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
            final short v = values.get(i);
            final long w = weights.get(i);

            if(!ShortPrimitives.isNull(v) && !LongPrimitives.isNull(w)){
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
    public static double tstat(Short... values) {
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
    public static double tstat(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return tstat(new DbShortArrayDirect(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(DbShortArray values) {
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
    public static double wtstat(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, double[] weights) {
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
    public static double wtstat(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, DbDoubleArray weights) {
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
    public static double wtstat(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, float[] weights) {
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
    public static double wtstat(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, DbFloatArray weights) {
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
    public static double wtstat(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, DbShortArray weights) {
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
    public static double wtstat(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, int[] weights) {
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
    public static double wtstat(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, DbIntArray weights) {
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
    public static double wtstat(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, long[] weights) {
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
    public static double wtstat(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbShortArray values, DbLongArray weights) {
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
    public static short max(DbShortArray values) {
        if (values == null) {
            return NULL_SHORT;
        }

        short val = NEG_INF_SHORT;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_SHORT : val;
    }

    /**
     * Returns the maximum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    public static short max(short[] values) {
        if (values == null) {
            return NULL_SHORT;
        }

        short val = NEG_INF_SHORT;
        long count = 0;
        for (short c : values) {
            if (!ShortPrimitives.isNull(c)) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_SHORT : val;
    }

    /**
     * Returns the maximum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return maximum of non-null values.
     */
    public static short max(Short... values) {
        if (values == null) {
            return NULL_SHORT;
        }

        short val = NEG_INF_SHORT;
        long count = 0;
        for (Short c : values) {
            if (!(c == null || ShortPrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_SHORT : val;
    }

    /**
     * Returns the minimum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static short min(DbShortArray values) {
        if (values == null) {
            return NULL_SHORT;
        }

        short val = POS_INF_SHORT;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_SHORT : val;
    }

    /**
     * Returns the minimum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static short min(short[] values) {
        if (values == null) {
            return NULL_SHORT;
        }

        short val = POS_INF_SHORT;
        long count = 0;
        for (short c : values) {
            if (!ShortPrimitives.isNull(c)) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_SHORT : val;
    }

    /**
     * Returns the minimum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return minimum of non-null values.
     */
    public static short min(Short... values) {
        if (values == null) {
            return NULL_SHORT;
        }

        short val = POS_INF_SHORT;
        long count = 0;
        for (Short c : values) {
            if (!(c == null || ShortPrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_SHORT : val;
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(Short... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }
        int n = values.length;
        if (n == 0) {
            return Double.NaN;
        } else {
            short[] copy = new short[values.length];

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
    public static double median(short[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return median(new DbShortArrayDirect(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(DbShortArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }
        int n = values.intSize("median");
        if (n == 0) {
            return Double.NaN;
        } else {
            short[] copy = new short[(int)values.size()];

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
    public static double percentile(double percentile, short... values) {
        if(values == null){
            return NULL_DOUBLE;
        }

        return percentile(percentile, new DbShortArrayDirect(values));
    }

    /**
     * Returns the percentile.
     *
     * @param percentile percentile to compute.
     * @param values values.
     * @return percentile.
     */
    public static double percentile(double percentile, DbShortArray values) {
        if(values == null){
            return NULL_DOUBLE;
        }

        if (percentile < 0 || percentile > 1)
            throw new RuntimeException("Invalid percentile = " + percentile);
        int n = values.intSize("percentile");

        short[] copy = new short[(int)values.size()];

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
    public static int firstIndexOf(short[] values, short val) {
        if (values == null) {
            return NULL_INT;
        }

        return firstIndexOf(new DbShortArrayDirect(values), val);
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static int firstIndexOf(DbShortArray values, short val) {
        if (values == null) {
            return NULL_INT;
        }

        final long L = values.size();
        for (int i = 0; i < L; ++i) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c) && c == val) {
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
    public static int indexOfMax(Short... values) {
        if (values == null) {
            return NULL_INT;
        }

        short val = NEG_INF_SHORT;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.length; i++) {
            Short c = values[i];
            if (c != null && !ShortPrimitives.isNull(c) && c > val) {
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
    public static int indexOfMax(short[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return indexOfMax(new DbShortArrayDirect(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(DbShortArray values) {
        if (values == null) {
            return NULL_INT;
        }

        short val = NEG_INF_SHORT;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c) && c > val) {
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
    public static int indexOfMin(Short... values) {
        if (values == null) {
            return NULL_INT;
        }

        short val = POS_INF_SHORT;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.length; i++) {
            Short c = values[i];
            if (c != null && !ShortPrimitives.isNull(c) && c < val) {
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
    public static int indexOfMin(short[] values) {
        if (values == null) {
            return NULL_INT;
        }

        return indexOfMin(new DbShortArrayDirect(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(DbShortArray values) {
        if (values == null) {
            return NULL_INT;
        }

        short val = POS_INF_SHORT;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c) && c < val) {
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
    public static int binSearchIndex(short[] values, short key, BinSearch choiceWhenEquals) {
        if (values == null) {
            return NULL_INT;
        }

        return binSearchIndex(new DbShortArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index immediately before where the key would be inserted.
     */
    public static int binSearchIndex(DbShortArray values, short key, BinSearch choiceWhenEquals) {
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
    public static int rawBinSearchIndex(short[] values, short key, BinSearch choiceWhenEquals) {
        if (values == null) {
            return NULL_INT;
        }

        return rawBinSearchIndex(new DbShortArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.
     * @param key key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(DbShortArray values, short key, BinSearch choiceWhenEquals) {
        if (values == null || key == NULL_SHORT) {
            return NULL_INT;
        }

        if (choiceWhenEquals != BinSearch.BS_ANY) {
            return binarySearch0Modified(values, 0, values.intSize("rawBinSearchIndex"), key, choiceWhenEquals == BinSearch.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, values.intSize("rawBinSearchIndex"), key);
        }
    }

    static private int binarySearch0(DbShortArray array, int fromIndex, int toIndex, short key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            short midVal = array.get(mid);
            if (midVal == NULL_SHORT) {
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

    static private int binarySearch0Modified(DbShortArray array, int fromIndex, int toIndex, short key, boolean highestOrLowest) {
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
            short midVal = array.get(mid);
            if (midVal == NULL_SHORT) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            if (key > midVal) {
                low = mid + 1;
                if (low <= high) {
                    short lowVal = array.get(low);
                    if (lowVal == NULL_SHORT) {
                        throw new RuntimeException("Can't have a null in the array!");
                    }
                    if (!highestOrLowest && key == lowVal) {
                        return low;
                    }
                }
            } else if (key < midVal) {
                high = mid - 1;
                if (high >= low) {
                    short highVal = array.get(high);
                    if (highVal == NULL_SHORT) {
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
    public static double cov(short[] values0, DbShortArray values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbShortArrayDirect(values0), values1);
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbShortArray values0, short[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(values0, new DbShortArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(short[] values0, short[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbShortArrayDirect(values0), new DbShortArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbShortArray values0, DbShortArray values1) {
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
            if (!ShortPrimitives.isNull(values0.get(i)) && !ShortPrimitives.isNull(values1.get(i))) {
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
    public static double cor(short[] values0, DbShortArray values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbShortArrayDirect(values0), values1);
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbShortArray values0, short[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(values0, new DbShortArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(short[] values0, short[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbShortArrayDirect(values0), new DbShortArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbShortArray values0, DbShortArray values1) {
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
            if (!ShortPrimitives.isNull(values0.get(i)) && !ShortPrimitives.isNull(values1.get(i))) {
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
    public static short sum(DbShortArray values) {
        if (values == null) {
            return NULL_SHORT;
        }

        double sum = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (short) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static short sum(short[] values) {
        if (values == null) {
            return NULL_SHORT;
        }

        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            short c = values[i];
            if (!ShortPrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (short) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return array of sums of non-null values.
     */
    public static short[] sum(DbArray<short[]> values) {
        if (values == null || values.size() == 0) {
            return null;
        }

        short[] result = new short[values.get(0).length];

        for (int j = 0; j < values.size(); j++) {
            short[] ai = values.get(j);
            Require.eq(ai.length, "a[i].length", result.length);

            for (int i = 0; i < ai.length; i++) {
                if (ShortPrimitives.isNull(result[i]) || ShortPrimitives.isNull(ai[i])) {
                    result[i] = NULL_SHORT;
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
    public static short[] sum(short[]... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        short[] result = new short[values[0].length];

        for (short[] v : values) {
            Require.eq(v.length, "a[i].length", result.length);

            for (int i = 0; i < v.length; i++) {
                if (ShortPrimitives.isNull(result[i]) || ShortPrimitives.isNull(v[i])) {
                    result[i] = NULL_SHORT;
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
    public static short product(DbShortArray values) {
        if (values == null) {
            return NULL_SHORT;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            short c = values.get(i);
            if (!ShortPrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_SHORT;
        }

        return (short) (prod);
    }

    /**
     * Returns the product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static short product(short[] values) {
        if (values == null) {
            return NULL_SHORT;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.length; i++) {
            short c = values[i];
            if (!ShortPrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_SHORT;
        }

        return (short) (prod);
    }

//    /**
//     * Returns the product.  Null values are excluded.  NaN values are included.
//     *
//     * @param values values.
//     * @return array of products of non-null values.
//     */
//    public static short[] product(DbArray<short[]> values) {
//        if (values == null || values.size() == 0) {
//            return null;
//        }
//
//        short[] result = new short[values.get(0).length];
//
//        for (int j = 0; j < values.size(); j++) {
//            short[] ai = values.get(j);
//            Require.eq(ai.length, "a[i].length", result.length);
//
//            for (int i = 0; i < ai.length; i++) {
//                if (ShortPrimitives.isNull(result[i]) || ShortPrimitives.isNull(ai[i])) {
//                    result[i] = NULL_SHORT;
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
//    public static short[] product(short[]... values) {
//        if (values == null || values.length == 0) {
//            return null;
//        }
//
//        short[] result = new short[values[0].length];
//
//        for (short[] v : values) {
//            Require.eq(v.length, "a[i].length", result.length);
//
//            for (int i = 0; i < v.length; i++) {
//                if (ShortPrimitives.isNull(result[i]) || ShortPrimitives.isNull(v[i])) {
//                    result[i] = NULL_SHORT;
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
    public static short[] cumsum(Short... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0];
        }

        short[] result = new short[values.length];
        result[0] = values[0] == null ? NULL_SHORT : values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i-1] == NULL_SHORT || ShortPrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (values[i] == null || ShortPrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (short) (result[i - 1] + values[i]);
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
    public static short[] cumsum(short[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0];
        }

        short[] result = new short[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (ShortPrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (ShortPrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (short) (result[i - 1] + values[i]);
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
    public static short[] cumsum(DbShortArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new short[0];
        }

        short[] result = new short[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (ShortPrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (ShortPrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (short) (result[i - 1] + values.get(i));
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
    public static short[] cumprod(Short... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0];
        }

        short[] result = new short[values.length];
        result[0] = values[0] == null ? NULL_SHORT : values[0];

        for (int i = 1; i < values.length; i++) {
            if (result[i-1] == NULL_SHORT || ShortPrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (values[i] == null || ShortPrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (short) (result[i - 1] * values[i]);
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
    public static short[] cumprod(short[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new short[0];
        }

        short[] result = new short[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (ShortPrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (ShortPrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (short) (result[i - 1] * values[i]);
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
    public static short[] cumprod(DbShortArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new short[0];
        }

        short[] result = new short[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (ShortPrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (ShortPrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (short) (result[i - 1] * values.get(i));
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
    public static short abs(short value) {
        if (ShortPrimitives.isNull(value)) {
            return NULL_SHORT;
        }

        return (short) Math.abs(value);
    }

    /**
     * Returns the arc cosine.
     *
     * @param value value.
     * @return arc cosine.
     */
    public static double acos(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double asin(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double atan(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double ceil(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double cos(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double exp(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double floor(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double log(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double pow(short a, short b) {
        if (ShortPrimitives.isNull(a) || ShortPrimitives.isNull(b)) {
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
    public static double rint(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static long round(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static short signum(short value) {
        if (ShortPrimitives.isNull(value)) {
            return NULL_SHORT;
        }

        return (short) Integer.signum(value);
    }

    /**
     * Returns the sine.
     *
     * @param value value.
     * @return sine.
     */
    public static double sin(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double sqrt(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static double tan(short value) {
        if (ShortPrimitives.isNull(value)) {
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
    public static short lowerBin(short value, short interval) {
        if (value == NULL_SHORT || interval == NULL_SHORT) {
            return NULL_SHORT;
        }

        return (short) (interval * ((short) Math.floor(value / interval)));
    }

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return lower bound of the bin containing the value.
     */
    public static short lowerBin(short value, short interval, short offset) {
        if (value == NULL_SHORT || interval == NULL_SHORT) {
            return NULL_SHORT;
        }

        return (short)(lowerBin((short)(value-offset),interval) + offset);
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @return upper bound of the bin containing the value.
     */
    public static short upperBin(short value, short interval) {
        if (value == NULL_SHORT || interval == NULL_SHORT) {
            return NULL_SHORT;
        }

        final double r = ((double) value) / ((double) interval);

        if (r == Math.round(r)) {
            return (short) (interval * r);
        }

        return (short) (interval * ((short) Math.floor(r + 1)));
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return upper bound of the bin containing the value.
     */
    public static short upperBin(short value, short interval, short offset) {
        if (value == NULL_SHORT || interval == NULL_SHORT) {
            return NULL_SHORT;
        }

        return (short)(upperBin((short)(value-offset),interval) + offset);
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
    public static short clamp(short value, short min, short max) {
        Require.leq(min, "min", max, "max");

        if (ShortPrimitives.isNull(value)) {
            return NULL_SHORT;
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
    public static double wsum(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, DbShortArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            short w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static double wsum(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, long[] weights) {
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
    public static double wsum(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, DbLongArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, long[] weights) {
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
    public static double weightedSum(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            long w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double wsum(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, double[] weights) {
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
    public static double wsum(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, DbDoubleArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, double[] weights) {
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
    public static double weightedSum(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            double w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wsum(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, float[] weights) {
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
    public static double wsum(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, DbFloatArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, float[] weights) {
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
    public static double weightedSum(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            float w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wsum(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, int[] weights) {
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
    public static double wsum(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbShortArray values, DbIntArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, int[] weights) {
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
    public static double weightedSum(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbShortArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            int w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wavg(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, double[] weights) {
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
    public static double wavg(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, DbDoubleArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(short[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, double[] weights) {
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
    public static double weightedAvg(short[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            double w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wavg(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, float[] weights) {
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
    public static double wavg(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, DbFloatArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(short[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, float[] weights) {
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
    public static double weightedAvg(short[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            float w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wavg(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, long[] weights) {
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
    public static double wavg(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, DbLongArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(short[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, long[] weights) {
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
    public static double weightedAvg(short[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        long wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            long w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double wavg(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, int[] weights) {
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
    public static double wavg(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, DbIntArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(short[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, int[] weights) {
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
    public static double weightedAvg(short[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        int wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            int w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wavg(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbShortArray values, DbShortArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(short[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(short[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbShortArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbShortArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        int wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            short c = values.get(i);
            short w = weights.get(i);
            if (!ShortPrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static DbShortArray sort(final DbShortArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbShortArrayDirect();
        }

        final short[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        return new DbShortArrayDirect(vs);
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static short[] sort(final short[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new short[]{};
        }

        final short[] vs = Arrays.copyOf(values, values.length);
        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static short[] sort(final Short... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new short[]{};
        }

        final short[] vs = new short[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_SHORT : values[i];
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
    public static DbShortArray sortDescending(final DbShortArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbShortArrayDirect();
        }

        final short[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return new DbShortArrayDirect(vs);
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static short[] sortDescending(final short[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new short[]{};
        }

        final short[] vs = Arrays.copyOf(values, values.length);
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
    public static short[] sortDescending(final Short... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new short[]{};
        }

        final short[] vs = new short[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_SHORT : values[i];
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
    public static short[] sequence(short start, short end, short step) {
        if(step == 0) {
            return new short[0];
        }

        final int n = (int)((end-start)/step);

        if(n < 0) {
            return new short[0];
        }

        final short[] result = new short[n+1];

        for(int i=0; i<=n; i++){
            result[i] = (short)(start + i*step);
        }

        return result;
    }

}
