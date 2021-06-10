/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatNumericPrimitives and regenerate
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
import static io.deephaven.libs.primitives.DoublePrimitives.unbox;

/**
 * A set of commonly used numeric functions that can be applied to Double types.
 */
public class DoubleNumericPrimitives {

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(Double... values) {
        return countPos(unbox(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(double[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return countPos(new DbDoubleArrayDirect(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static int countPos(DbDoubleArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c) && c > 0) {
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
    public static int countNeg(Double... values) {
        return countNeg(unbox(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(double[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return countNeg(new DbDoubleArrayDirect(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static int countNeg(DbDoubleArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c) && c < 0) {
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
    public static int countZero(Double... values) {
        return countZero(unbox(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(double[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return countZero(new DbDoubleArrayDirect(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static int countZero(DbDoubleArray values) {
        if (values == null) {
            return NULL_INT;
        }

        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c) && c == 0) {
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
    public static double avg(Double... values) {
        return avg(unbox(values));
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return avg(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the mean.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c)) {
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
    public static double absAvg(Double... values) {
        return absAvg(unbox(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return absAvg(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c)) {
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
    public static double var(Double... values) {
        return var(unbox(values));
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return var(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c)) {
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
    public static double wvar(double[] values, DbDoubleArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, double[] weights) {
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
    public static double wvar(double[] values, double[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            double c = values.get(i);
            double w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wvar(double[] values, DbFloatArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, float[] weights) {
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
    public static double wvar(double[] values, float[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            double c = values.get(i);
            float w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wvar(double[] values, DbShortArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, short[] weights) {
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
    public static double wvar(double[] values, short[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            double c = values.get(i);
            short w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static double wvar(double[] values, DbIntArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, int[] weights) {
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
    public static double wvar(double[] values, int[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            double c = values.get(i);
            int w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wvar(double[] values, DbLongArray weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, long[] weights) {
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
    public static double wvar(double[] values, long[] weights) {
        if(values == null || weights == null){
            return NULL_DOUBLE;
        }

        return wvar(new DbDoubleArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(DbDoubleArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        final long L = Math.min(values.size(), weights.size());
        for (int i = 0; i < L; i++) {
            double c = values.get(i);
            long w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double std(Double... values) {
        return std(unbox(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return std(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(DbDoubleArray values) {
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
    public static double wstd(double[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, double[] weights) {
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
    public static double wstd(double[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, DbDoubleArray weights) {
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
    public static double wstd(double[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, float[] weights) {
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
    public static double wstd(double[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, DbFloatArray weights) {
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
    public static double wstd(double[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, short[] weights) {
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
    public static double wstd(double[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, DbShortArray weights) {
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
    public static double wstd(double[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, int[] weights) {
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
    public static double wstd(double[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, DbIntArray weights) {
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
    public static double wstd(double[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, long[] weights) {
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
    public static double wstd(double[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new DbDoubleArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(DbDoubleArray values, DbLongArray weights) {
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
    public static double ste(Double... values) {
        return ste(unbox(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return ste(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final int c = DoublePrimitives.count(values);
        return s == NULL_DOUBLE || c == NULL_INT ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(double[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, double[] weights) {
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
    public static double wste(double[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, DbDoubleArray weights) {
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
            final double v = values.get(i);
            final double w = weights.get(i);

            if(!DoublePrimitives.isNull(v) && !DoublePrimitives.isNull(w)){
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
    public static double wste(double[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, float[] weights) {
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
    public static double wste(double[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, DbFloatArray weights) {
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
            final double v = values.get(i);
            final float w = weights.get(i);

            if(!DoublePrimitives.isNull(v) && !FloatPrimitives.isNull(w)){
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
    public static double wste(double[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, short[] weights) {
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
    public static double wste(double[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, DbShortArray weights) {
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
            final double v = values.get(i);
            final short w = weights.get(i);

            if(!DoublePrimitives.isNull(v) && !ShortPrimitives.isNull(w)){
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
    public static double wste(double[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, int[] weights) {
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
    public static double wste(double[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, DbIntArray weights) {
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
            final double v = values.get(i);
            final int w = weights.get(i);

            if(!DoublePrimitives.isNull(v) && !IntegerPrimitives.isNull(w)){
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
    public static double wste(double[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, long[] weights) {
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
    public static double wste(double[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new DbDoubleArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(DbDoubleArray values, DbLongArray weights) {
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
            final double v = values.get(i);
            final long w = weights.get(i);

            if(!DoublePrimitives.isNull(v) && !LongPrimitives.isNull(w)){
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
    public static double tstat(Double... values) {
        return tstat(unbox(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return tstat(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(DbDoubleArray values) {
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
    public static double wtstat(double[] values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, double[] weights) {
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
    public static double wtstat(double[] values, double[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, DbDoubleArray weights) {
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
    public static double wtstat(double[] values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, float[] weights) {
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
    public static double wtstat(double[] values, float[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, DbFloatArray weights) {
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
    public static double wtstat(double[] values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, short[] weights) {
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
    public static double wtstat(double[] values, short[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, DbShortArray weights) {
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
    public static double wtstat(double[] values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, int[] weights) {
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
    public static double wtstat(double[] values, int[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, DbIntArray weights) {
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
    public static double wtstat(double[] values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, long[] weights) {
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
    public static double wtstat(double[] values, long[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new DbDoubleArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(DbDoubleArray values, DbLongArray weights) {
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
    public static double max(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double val = NEG_INF_DOUBLE;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!(Double.isNaN(c) || DoublePrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_DOUBLE : val;
    }

    /**
     * Returns the maximum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return maximum of non-null, non-NaN values.
     */
    public static double max(double[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double val = NEG_INF_DOUBLE;
        long count = 0;
        for (double c : values) {
            if (!(Double.isNaN(c) || DoublePrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_DOUBLE : val;
    }

    /**
     * Returns the maximum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return maximum of non-null, non-NaN values.
     */
    public static double max(Double... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double val = NEG_INF_DOUBLE;
        long count = 0;
        for (Double c : values) {
            if (!(c == null || Double.isNaN(c) || DoublePrimitives.isNull(c))) {
                val = c > val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_DOUBLE : val;
    }

    /**
     * Returns the minimum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return minimum of non-null, non-NaN values.
     */
    public static double min(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double val = POS_INF_DOUBLE;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!(Double.isNaN(c) || DoublePrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_DOUBLE : val;
    }

    /**
     * Returns the minimum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return minimum of non-null, non-NaN values.
     */
    public static double min(double[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double val = POS_INF_DOUBLE;
        long count = 0;
        for (double c : values) {
            if (!(Double.isNaN(c) || DoublePrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_DOUBLE : val;
    }

    /**
     * Returns the minimum.  Null and NaN values are excluded.
     *
     * @param values values.
     * @return minimum of non-null, non-NaN values.
     */
    public static double min(Double... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double val = POS_INF_DOUBLE;
        long count = 0;
        for (Double c : values) {
            if (!(c == null || Double.isNaN(c) || DoublePrimitives.isNull(c))) {
                val = c < val ? c : val;
                count++;
            }
        }

        return count == 0 ? NULL_DOUBLE : val;
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(Double... values) {
        return median(unbox(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(double[] values) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return median(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }
        int n = values.intSize("median");
        if (n == 0) {
            return Double.NaN;
        } else {
            double[] copy = values.toArray();
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
    public static double percentile(double[] values, double percentile) {
        if( values == null) {
            return NULL_DOUBLE;
        }

        return percentile(new DbDoubleArrayDirect(values), percentile);
    }

    /**
     * Returns the percentile.
     *
     * @param values values.
     * @param percentile percentile to compute.
     * @return percentile.
     */
    public static double percentile(DbDoubleArray values, double percentile) {
        if(values == null){
            return NULL_DOUBLE;
        }

        if (percentile < 0 || percentile > 1)
            throw new RuntimeException("Invalid percentile = " + percentile);
        int n = values.intSize("percentile");
        double[] copy = values.toArray();
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
    public static int firstIndexOf(double[] values, double val) {
        if( values == null) {
            return NULL_INT;
        }

        return firstIndexOf(new DbDoubleArrayDirect(values), val);
    }

    /**
     * Returns the first index containing the value.
     *
     * @param values values.
     * @param val    value to search for.
     * @return first index containing the value or null, if the value is not present.
     */
    public static int firstIndexOf(DbDoubleArray values, double val) {
        if (values == null) {
            return NULL_INT;
        }

        final long L = values.size();
        for (int i = 0; i < L; ++i) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c) && c == val) {
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
    public static int indexOfMax(Double... values) {
        return indexOfMax(unbox(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(double[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return indexOfMax(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static int indexOfMax(DbDoubleArray values) {
        if (values == null) {
            return NULL_INT;
        }

        double val = NEG_INF_DOUBLE;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c) && c > val) {
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
    public static int indexOfMin(Double... values) {
        return indexOfMin(unbox(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(double[] values) {
        if( values == null) {
            return NULL_INT;
        }

        return indexOfMin(new DbDoubleArrayDirect(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static int indexOfMin(DbDoubleArray values) {
        if (values == null) {
            return NULL_INT;
        }

        double val = POS_INF_DOUBLE;
        int index = -1;
        long count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c) && c < val) {
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
    public static int binSearchIndex(double[] values, double key, BinSearch choiceWhenEquals) {
        if( values == null) {
            return NULL_INT;
        }

        return binSearchIndex(new DbDoubleArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    public static int binSearchIndex(DbDoubleArray values, double key, BinSearch choiceWhenEquals) {
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
    public static int rawBinSearchIndex(double[] values, double key, BinSearch choiceWhenEquals) {
        if( values == null) {
            return NULL_INT;
        }

        return rawBinSearchIndex(new DbDoubleArrayDirect(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values           sorted values to search.
     * @param key              key to search for.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(DbDoubleArray values, double key, BinSearch choiceWhenEquals) {
        if (values == null || key == NULL_DOUBLE) {
            return NULL_INT;
        }

        if (choiceWhenEquals != BinSearch.BS_ANY) {
            return binarySearch0Modified(values, 0, values.intSize("rawBinSearchIndex"), key, choiceWhenEquals == BinSearch.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, values.intSize("rawBinSearchIndex"), key);
        }
    }

    static private int binarySearch0(DbDoubleArray array, int fromIndex, int toIndex, double key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            double midVal = array.get(mid);
            if (midVal == NULL_DOUBLE || Double.isNaN(midVal)) {
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

    static private int binarySearch0Modified(DbDoubleArray array, int fromIndex, int toIndex, double key, boolean highestOrLowest) {
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
            double midVal = array.get(mid);
            if (midVal == NULL_DOUBLE || Double.isNaN(midVal)) {
                throw new RuntimeException("Can't have a null/NaN in the array!");
            }

            if (key > midVal) {
                low = mid + 1;
                if (low <= high) {
                    double lowVal = array.get(low);
                    if (lowVal == NULL_DOUBLE || Double.isNaN(midVal)) {
                        throw new RuntimeException("Can't have a null/NaN in the array!");
                    }
                    if (!highestOrLowest && key == lowVal) {
                        return low;
                    }
                }
            } else if (key < midVal) {
                high = mid - 1;
                if (high >= low) {
                    double highVal = array.get(high);
                    if (highVal == NULL_DOUBLE || Double.isNaN(midVal)) {
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
    public static double cov(double[] values0, DbDoubleArray values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbDoubleArrayDirect(values0), values1);
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbDoubleArray values0, double[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(values0, new DbDoubleArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(double[] values0, double[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new DbDoubleArrayDirect(values0), new DbDoubleArrayDirect(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(DbDoubleArray values0, DbDoubleArray values1) {
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
            if (!DoublePrimitives.isNull(values0.get(i)) && !DoublePrimitives.isNull(values1.get(i))) {
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
    public static double cor(double[] values0, DbDoubleArray values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbDoubleArrayDirect(values0), values1);
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbDoubleArray values0, double[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(values0, new DbDoubleArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(double[] values0, double[] values1) {
        if( values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new DbDoubleArrayDirect(values0), new DbDoubleArrayDirect(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.  NaN values are included.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(DbDoubleArray values0, DbDoubleArray values1) {
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
            if (!DoublePrimitives.isNull(values0.get(i)) && !DoublePrimitives.isNull(values1.get(i))) {
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
    public static double sum(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (double) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static double sum(double[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            double c = values[i];
            if (!DoublePrimitives.isNull(c)) {
                sum += c;
            }
        }
        return (double) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return array of sums of non-null values.
     */
    public static double[] sum(DbArray<double[]> values) {
        if (values == null || values.size() == 0) {
            return null;
        }

        double[] result = new double[values.get(0).length];

        for (int j = 0; j < values.size(); j++) {
            double[] ai = values.get(j);
            Require.eq(ai.length, "a[i].length", result.length);

            for (int i = 0; i < ai.length; i++) {
                if (DoublePrimitives.isNull(result[i]) || DoublePrimitives.isNull(ai[i])) {
                    result[i] = NULL_DOUBLE;
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
    public static double[] sum(double[]... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        double[] result = new double[values[0].length];

        for (double[] v : values) {
            Require.eq(v.length, "a[i].length", result.length);

            for (int i = 0; i < v.length; i++) {
                if (DoublePrimitives.isNull(result[i]) || DoublePrimitives.isNull(v[i])) {
                    result[i] = NULL_DOUBLE;
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
    public static double product(DbDoubleArray values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.size(); i++) {
            double c = values.get(i);
            if (!DoublePrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_DOUBLE;
        }

        return (double) (prod);
    }

    /**
     * Returns the product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static double product(double[] values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double prod = 1;
        int count = 0;
        for (int i = 0; i < values.length; i++) {
            double c = values[i];
            if (!DoublePrimitives.isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return NULL_DOUBLE;
        }

        return (double) (prod);
    }

//    /**
//     * Returns the product.  Null values are excluded.  NaN values are included.
//     *
//     * @param values values.
//     * @return array of products of non-null values.
//     */
//    public static double[] product(DbArray<double[]> values) {
//        if (values == null || values.size() == 0) {
//            return null;
//        }
//
//        double[] result = new double[values.get(0).length];
//
//        for (int j = 0; j < values.size(); j++) {
//            double[] ai = values.get(j);
//            Require.eq(ai.length, "a[i].length", result.length);
//
//            for (int i = 0; i < ai.length; i++) {
//                if (DoublePrimitives.isNull(result[i]) || DoublePrimitives.isNull(ai[i])) {
//                    result[i] = NULL_DOUBLE;
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
//    public static double[] product(double[]... values) {
//        if (values == null || values.length == 0) {
//            return null;
//        }
//
//        double[] result = new double[values[0].length];
//
//        for (double[] v : values) {
//            Require.eq(v.length, "a[i].length", result.length);
//
//            for (int i = 0; i < v.length; i++) {
//                if (DoublePrimitives.isNull(result[i]) || DoublePrimitives.isNull(v[i])) {
//                    result[i] = NULL_DOUBLE;
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
    public static double[] cumsum(Double... values) {
        return cumsum(unbox(values));
    }

    /**
     * Returns the cumulative sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static double[] cumsum(double[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        double[] result = new double[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (DoublePrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (DoublePrimitives.isNull(values[i])) {
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
    public static double[] cumsum(DbDoubleArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new double[0];
        }

        double[] result = new double[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (DoublePrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (DoublePrimitives.isNull(values.get(i))) {
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
    public static double[] cumprod(Double... values) {
        return cumprod(DoublePrimitives.unbox(values));
    }

    /**
     * Returns the cumulative product.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static double[] cumprod(double[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new double[0];
        }

        double[] result = new double[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (DoublePrimitives.isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (DoublePrimitives.isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (double) (result[i - 1] * values[i]);
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
    public static double[] cumprod(DbDoubleArray values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new double[0];
        }

        double[] result = new double[values.intSize("cumsum")];
        result[0] = values.get(0);

        for (int i = 1; i < values.size(); i++) {
            if (DoublePrimitives.isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (DoublePrimitives.isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (double) (result[i - 1] * values.get(i));
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
    public static double abs(double value) {
        if (DoublePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return (double) Math.abs(value);
    }

    /**
     * Returns the arc cosine.
     *
     * @param value value.
     * @return arc cosine.
     */
    public static double acos(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double asin(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double atan(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double ceil(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double cos(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double exp(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double floor(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double log(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double pow(double a, double b) {
        if (DoublePrimitives.isNull(a) || DoublePrimitives.isNull(b)) {
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
    public static double rint(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static long round(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double signum(double value) {
        if (DoublePrimitives.isNull(value)) {
            return NULL_DOUBLE;
        }

        return (double) Math.signum(value);
    }

    /**
     * Returns the sine.
     *
     * @param value value.
     * @return sine.
     */
    public static double sin(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double sqrt(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double tan(double value) {
        if (DoublePrimitives.isNull(value)) {
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
    public static double lowerBin(double value, double interval) {
        if (value == NULL_DOUBLE || interval == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        return (double) (interval * ((double) Math.floor(value / interval)));
    }

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return lower bound of the bin containing the value.
     */
    public static double lowerBin(double value, double interval, double offset) {
        if (value == NULL_DOUBLE || interval == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        return (double) (lowerBin((double) (value-offset),interval) + offset);
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @return upper bound of the bin containing the value.
     */
    public static double upperBin(double value, double interval) {
        if (value == NULL_DOUBLE || interval == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        final double r = ((double) value) / ((double) interval);

        if (r == Math.round(r)) {
            return (double) (interval * r);
        }

        return (double) (interval * ((double) Math.floor(r + 1)));
    }

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return upper bound of the bin containing the value.
     */
    public static double upperBin(double value, double interval, double offset) {
        if (value == NULL_DOUBLE || interval == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        return (double)(upperBin((double) (value-offset),interval) + offset);
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
    public static double clamp(double value, double min, double max) {
        Require.leq(min, "min", max, "max");

        if (DoublePrimitives.isNull(value)) {
            return NULL_DOUBLE;
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
    public static double wsum(double[] values, DbDoubleArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, DbDoubleArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, double[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, double[] weights) {
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
    public static double wsum(double[] values, double[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, double[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, DbDoubleArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            double w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wsum(double[] values, DbFloatArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, DbFloatArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, float[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, float[] weights) {
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
    public static double wsum(double[] values, float[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, float[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, DbFloatArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            float w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wsum(double[] values, DbLongArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, DbLongArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, long[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, long[] weights) {
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
    public static double wsum(double[] values, long[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, long[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, DbLongArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            long w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double wsum(double[] values, DbIntArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, DbIntArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, int[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, int[] weights) {
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
    public static double wsum(double[] values, int[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, int[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, DbIntArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            int w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wsum(double[] values, DbShortArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, DbShortArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, short[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, short[] weights) {
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
    public static double wsum(double[] values, short[] weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(double[] values, short[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedSum(new DbDoubleArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(DbDoubleArray values, DbShortArray weights) {
        return weightedSum(values, weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double weightedSum(DbDoubleArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            short w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static double wavg(double[] values, DbDoubleArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, DbDoubleArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, double[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, double[] weights) {
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
    public static double wavg(double[] values, double[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, double[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), new DbDoubleArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, DbDoubleArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, DbDoubleArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            double w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !DoublePrimitives.isNull(w)) {
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
    public static double wavg(double[] values, DbFloatArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, DbFloatArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, float[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, float[] weights) {
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
    public static double wavg(double[] values, float[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, float[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), new DbFloatArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, DbFloatArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, DbFloatArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            float w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !FloatPrimitives.isNull(w)) {
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
    public static double wavg(double[] values, DbLongArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, DbLongArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, long[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, long[] weights) {
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
    public static double wavg(double[] values, long[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, long[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), new DbLongArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, DbLongArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, DbLongArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        long wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            long w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !LongPrimitives.isNull(w)) {
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
    public static double wavg(double[] values, DbIntArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, DbIntArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, int[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, int[] weights) {
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
    public static double wavg(double[] values, int[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, int[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), new DbIntArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, DbIntArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, DbIntArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        int wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            int w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !IntegerPrimitives.isNull(w)) {
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
    public static double wavg(double[] values, DbShortArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, DbShortArray weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, short[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, short[] weights) {
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
    public static double wavg(double[] values, short[] weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(double[] values, short[] weights) {
        if( values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return weightedAvg(new DbDoubleArrayDirect(values), new DbShortArrayDirect(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(DbDoubleArray values, DbShortArray weights) {
        return weightedAvg(values, weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.  NaN values are included.
     *
     * @param values  values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double weightedAvg(DbDoubleArray values, DbShortArray weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        double vsum = 0;
        double wsum = 0;
        for (int i = 0; i < Math.min(values.size(), weights.size()); i++) {
            double c = values.get(i);
            short w = weights.get(i);
            if (!DoublePrimitives.isNull(c) && !ShortPrimitives.isNull(w)) {
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
    public static DbDoubleArray sort(final DbDoubleArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbDoubleArrayDirect();
        }

        final double[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        return new DbDoubleArrayDirect(vs);
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static double[] sort(final double[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new double[]{};
        }

        final double[] vs = Arrays.copyOf(values, values.length);
        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static double[] sort(final Double... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new double[]{};
        }

        final double[] vs = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_DOUBLE : values[i];
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
    public static DbDoubleArray sortDescending(final DbDoubleArray values) {
        if (values == null) {
            return null;
        }
        if (values.size() == 0) {
            return new DbDoubleArrayDirect();
        }

        final double[] vs = Arrays.copyOf(values.toArray(), values.intSize());
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return new DbDoubleArrayDirect(vs);
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static double[] sortDescending(final double[] values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new double[]{};
        }

        final double[] vs = Arrays.copyOf(values, values.length);
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
    public static double[] sortDescending(final Double... values) {
        if (values == null) {
            return null;
        }
        if (values.length == 0) {
            return new double[]{};
        }

        final double[] vs = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = ObjectPrimitives.isNull(values[i]) ? NULL_DOUBLE : values[i];
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
    public static double[] sequence(double start, double end, double step) {
        if(step == 0) {
            return new double[0];
        }

        final int n = (int)((end-start)/step);

        if(n < 0) {
            return new double[0];
        }

        final double[] result = new double[n+1];

        for(int i=0; i<=n; i++){
            result[i] = (double)(start + i*step);
        }

        return result;
    }
}

