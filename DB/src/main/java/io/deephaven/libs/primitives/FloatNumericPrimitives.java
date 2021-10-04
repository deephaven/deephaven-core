/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.libs.primitives;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.dbarrays.*;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.libs.primitives.FloatPrimitives.unbox;

/**
 * A set of commonly used numeric functions that can be applied to Float types.
 */
public class FloatNumericPrimitives {

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(Float... values) {
        return countPos(unbox(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(float[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return countPos(new DbFloatArrayDirect(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(DbFloatArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c) && c > 0) {
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
    public static int countNeg(Float... values) {
        return countNeg(unbox(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(float[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return countNeg(new DbFloatArrayDirect(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(DbFloatArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c) && c < 0) {
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
    public static int countZero(Float... values) {
        return countZero(unbox(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(float[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return countZero(new DbFloatArrayDirect(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(DbFloatArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c) && c == 0) {
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
    public static double avg(Float... values) {
        return avg(unbox(values));
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return avg(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(DbFloatArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c)) {
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
    public static double absAvg(Float... values) {
        return absAvg(unbox(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return absAvg(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(DbFloatArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c)) {
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
    public static double var(Float... values) {
        return var(unbox(values));
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return var(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(DbFloatArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c)) {
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
    public static double wvar(float[] values, DbDoubleArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, double[] weights) {
        if(values == null || weights == null){
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
    public static double wvar(float[] values, double[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            float c = values.get(i);
            double w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wvar(float[] values, DbFloatArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, float[] weights) {
        if(values == null || weights == null){
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
    public static double wvar(float[] values, float[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            float c = values.get(i);
            float w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wvar(float[] values, DbShortArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, short[] weights) {
        if(values == null || weights == null){
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
    public static double wvar(float[] values, short[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            float c = values.get(i);
            short w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static double wvar(float[] values, DbIntArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, int[] weights) {
        if(values == null || weights == null){
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
    public static double wvar(float[] values, int[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            float c = values.get(i);
            int w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wvar(float[] values, DbLongArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, long[] weights) {
        if(values == null || weights == null){
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
    public static double wvar(float[] values, long[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbFloatArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbFloatArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            float c = values.get(i);
            long w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double std(Float... values) {
        return std(unbox(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return std(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(DbFloatArray values) {
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
    public static double wstd(float[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, double[] weights) {
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
    public static double wstd(float[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, DbDoubleArray weights) {
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
    public static double wstd(float[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, float[] weights) {
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
    public static double wstd(float[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, DbFloatArray weights) {
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
    public static double wstd(float[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, short[] weights) {
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
    public static double wstd(float[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, DbShortArray weights) {
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
    public static double wstd(float[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, int[] weights) {
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
    public static double wstd(float[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, DbIntArray weights) {
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
    public static double wstd(float[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, long[] weights) {
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
    public static double wstd(float[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbFloatArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbFloatArray values, DbLongArray weights) {
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
    public static double ste(Float... values) {
        return ste(unbox(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return ste(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(DbFloatArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final int c = FloatPrimitives.count(values);
        return s == NULL_DOUBLE || c == NULL_INT ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(float[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, double[] weights) {
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
    public static double wste(float[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, DbDoubleArray weights) {
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
            final float v = values.get(i);
            final double w = weights.get(i);

            if(!FloatPrimitives.isNull(v) && !DoublePrimitives.isNull(w)){
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
    public static double wste(float[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, float[] weights) {
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
    public static double wste(float[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, DbFloatArray weights) {
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
            final float v = values.get(i);
            final float w = weights.get(i);

            if(!FloatPrimitives.isNull(v) && !FloatPrimitives.isNull(w)){
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
    public static double wste(float[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, short[] weights) {
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
    public static double wste(float[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, DbShortArray weights) {
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
            final float v = values.get(i);
            final short w = weights.get(i);

            if(!FloatPrimitives.isNull(v) && !ShortPrimitives.isNull(w)){
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
    public static double wste(float[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, int[] weights) {
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
    public static double wste(float[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, DbIntArray weights) {
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
            final float v = values.get(i);
            final int w = weights.get(i);

            if(!FloatPrimitives.isNull(v) && !IntegerPrimitives.isNull(w)){
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
    public static double wste(float[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, long[] weights) {
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
    public static double wste(float[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbFloatArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbFloatArray values, DbLongArray weights) {
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
            final float v = values.get(i);
            final long w = weights.get(i);

            if(!FloatPrimitives.isNull(v) && !LongPrimitives.isNull(w)){
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
    public static double tstat(Float... values) {
        return tstat(unbox(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return tstat(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(DbFloatArray values) {
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
    public static double wtstat(float[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, double[] weights) {
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
    public static double wtstat(float[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, DbDoubleArray weights) {
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
    public static double wtstat(float[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, float[] weights) {
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
    public static double wtstat(float[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, DbFloatArray weights) {
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
    public static double wtstat(float[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, short[] weights) {
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
    public static double wtstat(float[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, DbShortArray weights) {
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
    public static double wtstat(float[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, int[] weights) {
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
    public static double wtstat(float[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, DbIntArray weights) {
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
    public static double wtstat(float[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, long[] weights) {
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
    public static double wtstat(float[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbFloatArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbFloatArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = weightedAvg(values, weights);
        final double s = wste(values, weights);
        return a / s;
    }

    /**
     * Returns the maximum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return maximum of non-null, non-NaN values.
     */
    public static float max(DbFloatArray values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        float val = NEGATIVE_INFINITY_FLOAT;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!(Float.isNaN(c) || FloatPrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_FLOAT : val;
    }

    /**
     * Returns the maximum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return maximum of non-null, non-NaN values.
     */
    public static float max(float[] values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        float val = NEGATIVE_INFINITY_FLOAT;
        long count = 0;
        for (float c : values) {
            if (!(Float.isNaN(c) || FloatPrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_FLOAT : val;
    }

    /**
     * Returns the maximum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return maximum of non-null, non-NaN values.
     */
    public static float max(Float... values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        float val = NEGATIVE_INFINITY_FLOAT;
        long count = 0;
        for (Float c : values) {
            if (!(c == null || Float.isNaN(c) || FloatPrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_FLOAT : val;
    }

    /**
     * Returns the minimum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return minimum of non-null, non-NaN values.
     */
    public static float min(DbFloatArray values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        float val = POSITIVE_INFINITY_FLOAT;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!(Float.isNaN(c) || FloatPrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_FLOAT : val;
    }

    /**
     * Returns the minimum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return minimum of non-null, non-NaN values.
     */
    public static float min(float[] values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        float val = POSITIVE_INFINITY_FLOAT;
        long count = 0;
        for (float c : values) {
            if (!(Float.isNaN(c) || FloatPrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_FLOAT : val;
    }

    /**
     * Returns the minimum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return minimum of non-null, non-NaN values.
     */
    public static float min(Float... values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        float val = POSITIVE_INFINITY_FLOAT;
        long count = 0;
        for (Float c : values) {
            if (!(c == null || Float.isNaN(c) || FloatPrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_FLOAT : val;
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(Float... values) {
        return median(unbox(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(float[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return median(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(DbFloatArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }
        int n = values.intSize("median");
        if (n == 0) {
            return Double.NaN;
        } else {
            float[] copy = values.toArray();
            Arrays.sort(copy);
            if (n % 2 == 0)
                return 0.5 * (copy[n / 2 - 1] + copy[n / 2]);
            else return copy[n / 2];
        }
    }

    /**
     * Returns the percentile.
     *
     * @param values values.
     * @param percentile percentile to compute.
     * @return percentile.
     */
    public static double percentile(float[] values, double percentile) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return percentile(new DbFloatArrayDirect(values), percentile);
    }

    /**
     * Returns the percentile.
     *
     * @param values values.
     * @param percentile percentile to compute.
     * @return percentile.
     */
    public static double percentile(DbFloatArray values, double percentile) {
        if(values == null){
            return NULL_DOUBLE;
        }

        if (percentile < 0 || percentile > 1)
            throw new RuntimeException("Invalid percentile = " + percentile);
        int n = values.intSize("percentile");
        float[] copy = values.toArray();
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
    public static int firstIndexOf(float[] values, float val) {
        if( values == null) {
            return NULL_INT;
        }

        return firstIndexOf(new DbFloatArrayDirect(values), val);
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val    value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static int firstIndexOf(DbFloatArray values, float val) {
        if (values == null) {
            return NULL_INT;
        }

        final long L = values.size();
        for (int i = 0; i < L; ++i) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c) && c == val) {
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
    public static int indexOfMax(Float... values) {
        return indexOfMax(unbox(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(float[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return indexOfMax(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(DbFloatArray values) {
        if (values == null) {
            return NULL_INT;
        }

        float val = NEGATIVE_INFINITY_FLOAT;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c) && c > val) {
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
    public static int indexOfMin(Float... values) {
        return indexOfMin(unbox(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(float[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return indexOfMin(new DbFloatArrayDirect(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(DbFloatArray values) {
        if (values == null) {
            return NULL_INT;
        }

        float val = POSITIVE_INFINITY_FLOAT;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c) && c < val) {
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
     * @param values sorted values to search.
     * @param key key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    public static int binSearchIndex(float[] values, float key, BinSearch choiceWhenEquals) {
        if( values == null) {
            return NULL_INT;
        }

        return binSearchIndex(new DbFloatArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    public static int binSearchIndex(DbFloatArray values, float key, BinSearch choiceWhenEquals) {
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
    public static int rawBinSearchIndex(float[] values, float key, BinSearch choiceWhenEquals) {
        if( values == null) {
            return NULL_INT;
        }

        return rawBinSearchIndex(new DbFloatArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(DbFloatArray values, float key, BinSearch choiceWhenEquals) {
        if (values == null || key == NULL_FLOAT) {
            return NULL_INT;
        }

        if (choiceWhenEquals != BinSearch.BS_ANY) {
            return binarySearch0Modified(values, 0, values.intSize("rawBinSearchIndex"), key, choiceWhenEquals == BinSearch.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, values.intSize("rawBinSearchIndex"), key);
        }
    }

    static private int binarySearch0(DbFloatArray array, int fromIndex, int toIndex, float key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            float midVal = array.get(mid);
            if (midVal == NULL_FLOAT || Float.isNaN(midVal)) {
                throw new RuntimeException("Can't have a null/NaN in the array!");
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

    static private int binarySearch0Modified(DbFloatArray array, int fromIndex, int toIndex, float key, boolean highestOrLowest) {
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
            float midVal = array.get(mid);
            if (midVal == NULL_FLOAT || Float.isNaN(midVal)) {
                throw new RuntimeException("Can't have a null/NaN in the array!");
            }

            if (key > midVal) {
                low = mid + 1;
                if (low <= high) {
                    float lowVal = array.get(low);
                    if (lowVal == NULL_FLOAT || Float.isNaN(midVal)) {
                        throw new RuntimeException("Can't have a null/NaN in the array!");
                    }
                    if (!highestOrLowest && key == lowVal) {
                        return low;
                    }
                }
            } else if (key < midVal) {
                high = mid - 1;
                if (high >= low) {
                    float highVal = array.get(high);
                    if (highVal == NULL_FLOAT || Float.isNaN(midVal)) {
                        throw new RuntimeException("Can't have a null/NaN in the array!");
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
    public static double cov(float[] values0, DbFloatArray values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbFloatArrayDirect(values0), values1);
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbFloatArray values0, float[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(values0, new DbFloatArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(float[] values0, float[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbFloatArrayDirect(values0), new DbFloatArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbFloatArray values0, DbFloatArray values1) {
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
            if (!FloatPrimitives.isNull(values0.get(i)) && !FloatPrimitives.isNull(values1.get(i))) {
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
    public static double cor(float[] values0, DbFloatArray values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbFloatArrayDirect(values0), values1);
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbFloatArray values0, float[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(values0, new DbFloatArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(float[] values0, float[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbFloatArrayDirect(values0), new DbFloatArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbFloatArray values0, DbFloatArray values1) {
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
            if (!FloatPrimitives.isNull(values0.get(i)) && !FloatPrimitives.isNull(values1.get(i))) {
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
    public static float sum(DbFloatArray values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        double sum = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (float) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static float sum(float[] values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            float c = values[i];
            if (!FloatPrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (float) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return array of sums of non-null values.
     */
    public static float[] sum(DbArray<float[]> values) {
        if (values == null || values.size() == 0) {
            return null;
        }

        float[] result = new float[values.get(0).length];

        for (int j = 0; j < values.size(); j++) {
            float[] ai = values.get(j);
            Require.eq(ai.length, "a[i].length", result.length);

            for (int i = 0; i < ai.length; i++) {
                if (FloatPrimitives.isNull(result[i]) || FloatPrimitives.isNull(ai[i])) {
                    result[i] = NULL_FLOAT;
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
    public static float[] sum(float[]... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        float[] result = new float[values[0].length];

        for (float[] v : values) {
            Require.eq(v.length, "a[i].length", result.length);

            for (int i = 0; i < v.length; i++) {
                if (FloatPrimitives.isNull(result[i]) || FloatPrimitives.isNull(v[i])) {
                    result[i] = NULL_FLOAT;
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
    public static float product(DbFloatArray values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            float c = values.get(i);
            if (!FloatPrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_FLOAT;
        }

        return (float) (prod);
    }

    /**
     * Returns the product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static float product(float[] values) {
        if (values == null) {
            return NULL_FLOAT;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.length; i++) {
            float c = values[i];
            if (!FloatPrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_FLOAT;
        }

        return (float) (prod);
    }

//    /**
//     * Returns the product.  Null values are excluded.  NaN values are included.
//     *
//     * @param values values.
//     * @return array of products of non-null values.
//     */
//    public static float[] product(DbArray<float[]> values) {
//        if (values == null || values.size() == 0) {
//            return null;
//        }
//
//        float[] result = new float[values.get(0).length];
//
//        for (int j = 0; j < values.size(); j++) {
//            float[] ai = values.get(j);
//            Require.eq(ai.length, "a[i].length", result.length);
//
//            for (int i = 0; i < ai.length; i++) {
//                if (FloatPrimitives.isNull(result[i]) || FloatPrimitives.isNull(ai[i])) {
//                    result[i] = NULL_FLOAT;
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
//    public static float[] product(float[]... values) {
//        if (values == null || values.length == 0) {
//            return null;
//        }
//
//        float[] result = new float[values[0].length];
//
//        for (float[] v : values) {
//            Require.eq(v.length, "a[i].length", result.length);
//
//            for (int i = 0; i < v.length; i++) {
//                if (FloatPrimitives.isNull(result[i]) || FloatPrimitives.isNull(v[i])) {
//                    result[i] = NULL_FLOAT;
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
    public static float[] cumsum(Float... values) {
        return cumsum(unbox(values));
    }

    /**
     * Returns the cumulative sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static float[] cumsum(float[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0];
        }

        float[] result = new float[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (FloatPrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (FloatPrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (result[i - 1] + values[i]);
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
    public static float[] cumsum(DbFloatArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new float[0];
        }

        float[] result = new float[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (FloatPrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (FloatPrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (result[i - 1] + values.get(i));
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
    public static float[] cumprod(Float... values) {
        return cumprod(FloatPrimitives.unbox(values));
    }

    /**
     * Returns the cumulative product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static float[] cumprod(float[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new float[0];
        }

        float[] result = new float[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (FloatPrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (FloatPrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (float) (result[i - 1] * values[i]);
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
    public static float[] cumprod(DbFloatArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new float[0];
        }

        float[] result = new float[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (FloatPrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (FloatPrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (float) (result[i - 1] * values.get(i));
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
    public static float abs(float value) {
        if (FloatPrimitives.isNull(value)) {
            return NULL_FLOAT;
        }

        return (float) Math.abs(value);
    }

    /**
     * Returns the arc cosine.
     *
     * @param value value.
     * @return arc cosine.
     */
    public static double acos(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double asin(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double atan(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double ceil(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double cos(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double exp(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double floor(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double log(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double pow(float a, float b) {
        if (FloatPrimitives.isNull(a) || FloatPrimitives.isNull(b)) {
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
    public static double rint(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static long round(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static float signum(float value) {
        if (FloatPrimitives.isNull(value)) {
            return NULL_FLOAT;
        }

        return (float) Math.signum(value);
    }

    /**
     * Returns the sine.
     *
     * @param value value.
     * @return sine.
     */
    public static double sin(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double sqrt(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static double tan(float value) {
        if (FloatPrimitives.isNull(value)) {
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
    public static float lowerBin(float value, float interval) {
        if (value == NULL_FLOAT || interval == NULL_FLOAT) {
            return NULL_FLOAT;
        }

        return (float) (interval * ((float) Math.floor(value / interval)));
    }

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return lower bound of the bin containing the value.
     */
    public static float lowerBin(float value, float interval, float offset) {
        if (value == NULL_FLOAT || interval == NULL_FLOAT) {
            return NULL_FLOAT;
        }

        return (float) (lowerBin((float) (value-offset),interval) + offset);
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @return upper bound of the bin containing the value.
     */
    public static float upperBin(float value, float interval) {
        if (value == NULL_FLOAT || interval == NULL_FLOAT) {
            return NULL_FLOAT;
        }

        final double r = ((double) value) / ((double) interval);

        if (r == Math.round(r)) {
            return (float) (interval * r);
        }

        return (float) (interval * ((float) Math.floor(r + 1)));
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return upper bound of the bin containing the value.
     */
    public static float upperBin(float value, float interval, float offset) {
        if (value == NULL_FLOAT || interval == NULL_FLOAT) {
            return NULL_FLOAT;
        }

        return (float)(upperBin((float) (value-offset),interval) + offset);
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
    public static float clamp(float value, float min, float max) {
        Require.leq(min, "min", max, "max");

        if (FloatPrimitives.isNull(value)) {
            return NULL_FLOAT;
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
    public static double wsum(float[] values, DbFloatArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, DbFloatArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, float[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, float[] weights) {
        if( values == null || weights == null) {
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
    public static double wsum(float[] values, float[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, float[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, DbFloatArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            float w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wsum(float[] values, DbDoubleArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, DbDoubleArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, double[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, double[] weights) {
        if( values == null || weights == null) {
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
    public static double wsum(float[] values, double[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, double[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, DbDoubleArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            double w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wsum(float[] values, DbLongArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, DbLongArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, long[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, long[] weights) {
        if( values == null || weights == null) {
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
    public static double wsum(float[] values, long[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, long[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, DbLongArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            long w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double wsum(float[] values, DbIntArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, DbIntArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, int[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, int[] weights) {
        if( values == null || weights == null) {
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
    public static double wsum(float[] values, int[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, int[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, DbIntArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            int w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wsum(float[] values, DbShortArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, DbShortArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, short[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, short[] weights) {
        if( values == null || weights == null) {
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
    public static double wsum(float[] values, short[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(float[] values, short[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbFloatArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbFloatArray values, DbShortArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbFloatArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            short w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
                vsum += c * w;
            }
        }
        return vsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, DbFloatArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, DbFloatArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, float[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, float[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, float[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, float[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, DbFloatArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        float wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            float w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, DbDoubleArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, DbDoubleArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, double[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, double[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, double[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, double[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, DbDoubleArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            double w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, DbLongArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, DbLongArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, long[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, long[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, long[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, long[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, DbLongArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        long wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            long w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, DbIntArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, DbIntArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, int[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, int[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, int[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, int[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, DbIntArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        int wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            int w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
        }
        return vsum / wsum;
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, DbShortArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, DbShortArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, short[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, short[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(values, new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(float[] values, short[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(float[] values, short[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbFloatArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbFloatArray values, DbShortArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbFloatArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            float c = values.get(i);
            short w = weights.get(i);
            if (!FloatPrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static DbFloatArray sort(final DbFloatArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbFloatArrayDirect();
        }

        final float[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        return new DbFloatArrayDirect(vs);
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static float[] sort(final float[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new float[]{};
        }

        final float[] vs = Arrays.copyOf(values, values.length);
        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static float[] sort(final Float... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new float[]{};
        }

        final float[] vs = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_FLOAT : values[i];
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
    public static DbFloatArray sortDescending(final DbFloatArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbFloatArrayDirect();
        }

        final float[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return new DbFloatArrayDirect(vs);
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static float[] sortDescending(final float[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new float[]{};
        }

        final float[] vs = Arrays.copyOf(values, values.length);
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
    public static float[] sortDescending(final Float... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new float[]{};
        }

        final float[] vs = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_FLOAT : values[i];
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
    public static float[] sequence(float start, float end, float step) {
        if(step == 0) {
            return new float[0];
        }

        final int n = (int)((end-start)/step);

        if(n < 0) {
            return new float[0];
        }

        final float[] result = new float[n+1];

        for(int i=0; i<=n; i++){
            result[i] = (float)(start + i*step);
        }

        return result;
    }
}

