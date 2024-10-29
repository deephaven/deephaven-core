<#--
  Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
-->

package io.deephaven.function;

import io.deephaven.base.verify.Require;
import io.deephaven.vector.*;
import io.deephaven.engine.primitive.iterator.*;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.util.Arrays;
import java.lang.Math;

import static io.deephaven.base.CompareUtils.compare;
import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Basic.*;
import static io.deephaven.function.Sort.*;
import static io.deephaven.function.Cast.castDouble;

/**
 * A set of commonly used numeric functions that can be applied to numeric types.
 */
@SuppressWarnings({"RedundantCast", "unused", "ManualMinMaxCalculation"})
public class Numeric {

    //////////////////////////// Constants ////////////////////////////

    /**
     * The double value that is closer than any other to e, the base of the natural logarithms.
     */
    static public final double E = Math.E;

    /**
     * The double value that is closer than any other to pi, the ratio of the circumference of a circle to its diameter.
     */
    static public final double PI = Math.PI;

    //////////////////////////// Object ////////////////////////////


    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values, or null if there are no non-null values.
     */
    static public <T extends Comparable<T>> T maxObj(ObjectVector<T> values) {
        final long idx = indexOfMaxObj(values);
        return idx == NULL_LONG ? null : values.get(idx);
    }

    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values, or null if there are no non-null values.
     */
    @SafeVarargs
    static public <T extends Comparable<T>> T maxObj(final T... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        return maxObj(new ObjectVectorDirect<>(values));
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values, or null if there are no non-null values.
     */
    static public <T extends Comparable<T>> T minObj(ObjectVector<T> values) {
        final long idx = indexOfMinObj(values);
        return idx == NULL_LONG ? null : values.get(idx);
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values, or null if there are no non-null values.
     */
    @SafeVarargs
    public static <T extends Comparable<T>> T minObj(final T... values) {
        if (values == null || values.length == 0) {
            return null;
        }

        return minObj(new ObjectVectorDirect<>(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    @SafeVarargs
    public static <T extends Comparable<T>> long indexOfMaxObj(T... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return indexOfMaxObj(new ObjectVectorDirect<>(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static <T extends Comparable<T>> long indexOfMaxObj(ObjectVector<T> values) {
        if (values == null) {
            return NULL_LONG;
        }

        T val = null;
        long index = NULL_LONG;
        long count = 0;
        long i = 0;

        try (final CloseableIterator<T> vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final T c = vi.next();
                if (!isNull(c) && ( val == null || c.compareTo(val) > 0)) {
                    val = c;
                    index = i;
                    count++;
                }

                i++;
            }
        }

        return count == 0 ? NULL_LONG : index;
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    @SafeVarargs
    public static <T extends Comparable<T>> long indexOfMinObj(T... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return indexOfMinObj(new ObjectVectorDirect<>(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static <T extends Comparable<T>> long indexOfMinObj(ObjectVector<T> values) {
        if (values == null) {
            return NULL_LONG;
        }

        T val = null;
        long index = NULL_LONG;
        long count = 0;
        long i = 0;

        try ( final CloseableIterator<T> vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final T c = vi.next();
                if (!isNull(c) && ( val == null || c.compareTo(val) < 0)) {
                    val = c;
                    index = i;
                    count++;
                }

                i++;
            }
        }

        return count == 0 ? NULL_LONG : index;
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    //////////////////////////// ${pt.primitive} ////////////////////////////


    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static long countPos(${pt.boxed}[] values) {
        return countPos(unbox(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static long countPos(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return countPos(new ${pt.vectorDirect}(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static long countPos(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long count = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                if (!isNull(c) && c > 0) {
                    count++;
                }
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
    public static long countNeg(${pt.boxed}[] values) {
        return countNeg(unbox(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static long countNeg(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return countNeg(new ${pt.vectorDirect}(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static long countNeg(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long count = 0;
        final long n = values.size();

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                if (!isNull(c) && c < 0) {
                    count++;
                }
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
    public static long countZero(${pt.boxed}[] values) {
        return countZero(unbox(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static long countZero(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return countZero(new ${pt.vectorDirect}(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static long countZero(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long count = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                if (!isNull(c) && c == 0) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * Returns the mean.  Null values are excluded.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(${pt.boxed}[] values) {
        return avg(unbox(values));
    }

    /**
     * Returns the mean.  Null values are excluded.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return avg(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the mean.  Null values are excluded.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        long nullCount = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(c)) {
                    return Double.NaN;
                }
                </#if>
                if (!isNull(c)) {
                    sum += c;
                    count++;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        return sum / count;
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(${pt.boxed}[] values) {
        return absAvg(unbox(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return absAvg(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        long nullCount = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(c)) {
                    return Double.NaN;
                }
                if (isInf(c)) {
                    return Double.POSITIVE_INFINITY;
                }
                </#if>
                if (!isNull(c)) {
                    sum += Math.abs(c);
                    count++;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        return sum / count;
    }

    /**
     * Returns the sample variance.  Null values are excluded.
     *
     * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @return sample variance of non-null values.
     */
    public static double var(${pt.boxed}[] values) {
        return var(unbox(values));
    }

    /**
     * Returns the sample variance.  Null values are excluded.
     *
     * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @return sample variance of non-null values.
     */
    public static double var(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return var(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the sample variance.  Null values are excluded.
     *
     * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @return sample variance of non-null values.
     */
    public static double var(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        long count = 0;
        long nullCount = 0;
        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(c) || isInf(c)) {
                    return Double.NaN;
                }
                </#if>
                if (!isNull(c)) {
                    sum += (double)c;
                    sum2 += (double)c * (double)c;
                    count++;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        // Return NaN if overflow or too few values to compute variance.
        if (count <= 1 || Double.isInfinite(sum) || Double.isInfinite(sum2)) {
            return Double.NaN;
        }

        // Perform the calculation in a way that minimizes the impact of floating point error.
        final double eps = Math.ulp(sum2);
        final double vs2bar = sum * (sum / (double)count);
        final double delta = sum2 - vs2bar;
        final double rel_eps = delta / eps;

        // Return zero when the sample variance is leq the floating point error.
        return Math.abs(rel_eps) > 1.0 ? delta / ((double)count - 1) : 0.0;
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted sample variance.  Null values are excluded.
     *
     * Weighted sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the weighted sample variance will be an unbiased estimator of weighted population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample variance of non-null values.
     */
    public static double wvar(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted sample variance.  Null values are excluded.
     *
     * Weighted sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the weighted sample variance will be an unbiased estimator of weighted population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample variance of non-null values.
     */
    public static double wvar(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new ${pt.vectorDirect}(values), weights);
    }

    /**
     * Returns the weighted sample variance.  Null values are excluded.
     *
     * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample variance of non-null values.
     */
    public static double wvar(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted sample variance.  Null values are excluded.
     *
     * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample variance of non-null values.
     */
    public static double wvar(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final long n = values.size();

        if (n != weights.size()) {
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;
        double count2 = 0;
        long nullCount = 0;
        long valueCount = 0;

        try (
            final ${pt.vectorIterator} vi = values.iterator();
            final ${pt2.vectorIterator} wi = weights.iterator()
        ) {
            while (vi.hasNext()) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                final ${pt2.primitive} w = wi.${pt2.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(c) || isInf(c)) {
                    return Double.NaN;
                }
                </#if>
                <#if pt2.valueType.isFloat >
                if (isNaN(w) || isInf(w)) {
                    return Double.NaN;
                }
                </#if>
                if (!isNull(c) && !isNull(w)) {
                    sum += w * c;
                    sum2 += w * c * c;
                    count += w;
                    count2 += w * w;
                    valueCount++;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        // Return NaN if overflow or too few values to compute variance.
        if (valueCount <= 1 || Double.isInfinite(sum) || Double.isInfinite(sum2)) {
            return Double.NaN;
        }

        // For unbiased estimator derivation see https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Weighted_sample_variance
        // For unweighted statistics, there is a (N-1)/N = 1-(1/N) Bessel correction.
        // The analagous correction for weighted statistics is 1-count2/count/count, which yields an effective sample size of Neff = count*count/count2.
        // This yields an unbiased estimator of (sum2/count - sum*sum/count/count) * ((count*count/count2)/((count*count/count2)-1)).
        // This can be simplified to (count * sum2 - sum * sum) / (count * count - count2)
        return (count * sum2 - sum * sum) / (count * count - count2);
    }

    </#if>
    </#list>


    /**
     * Returns the sample standard deviation.  Null values are excluded.
     *
     * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @return sample standard deviation of non-null values.
     */
    public static double std(${pt.boxed}[] values) {
        return std(unbox(values));
    }

    /**
     * Returns the sample standard deviation.  Null values are excluded.
     *
     * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @return sample standard deviation of non-null values.
     */
    public static double std(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return std(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the sample standard deviation.  Null values are excluded.
     *
     * Sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param values values.
     * @return sample standard deviation of non-null values.
     */
    public static double std(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double v = var(values);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted sample standard deviation.  Null values are excluded.
     *
     * Weighted sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the weighted sample variance will be an unbiased estimator of weighted population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample standard deviation of non-null values.
     */
    public static double wstd(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted sample standard deviation.  Null values are excluded.
     *
     * Weighted sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the weighted sample variance will be an unbiased estimator of weighted population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample standard deviation of non-null values.
     */
    public static double wstd(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new ${pt.vectorDirect}(values), weights);
    }

    /**
     * Returns the weighted sample standard deviation.  Null values are excluded.
     *
     * Weighted sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the weighted sample variance will be an unbiased estimator of weighted population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample standard deviation of non-null values.
     */
    public static double wstd(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted sample standard deviation.  Null values are excluded.
     *
     * Weighted sample standard deviation is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the weighted sample variance will be an unbiased estimator of weighted population variance.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sample standard deviation of non-null values.
     */
    public static double wstd(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double v = wvar(values, weights);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    </#if>
    </#list>


    /**
     * Returns the standard error.  Null values are excluded.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(${pt.boxed}[] values) {
        return ste(unbox(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return ste(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double s = std(values);
        final long c = count(values);
        return s == NULL_DOUBLE || c == NULL_LONG ? NULL_DOUBLE : s / Math.sqrt(c);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new ${pt.vectorDirect}(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if (values.size() != weights.size()) {
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        final double s = wstd(values, weights);
        if (s == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;

        try (
            final ${pt.vectorIterator} vi = values.iterator();
            final ${pt2.vectorIterator} wi = weights.iterator()
        ) {
            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();
                final ${pt2.primitive} w = wi.${pt2.iteratorNext}();

                if (!isNull(v) && !isNull(w)) {
                    sumw += w;
                    sumw2 += w*w;
                }
            }
        }

        return s * Math.sqrt(sumw2/sumw/sumw);
    }

    </#if>
    </#list>


    /**
     * Returns the t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(${pt.boxed}[] values) {
        return tstat(unbox(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return tstat(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double a = avg(values);
        final double s = ste(values);
        return a == NULL_DOUBLE || s == NULL_DOUBLE ? NULL_DOUBLE : avg(values) / ste(values);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new ${pt.vectorDirect}(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = wavg(values, weights);
        if (a == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        final double s = wste(values, weights);
        if (s == NULL_DOUBLE) {
            return NULL_DOUBLE;
        }

        return a / s;
    }

    </#if>
    </#list>


    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values, or null if there are no non-null values.
     */
    public static ${pt.primitive} max(${pt.vector} values) {
        final long idx = indexOfMax(values);
        return idx == NULL_LONG ? ${pt.null} : values.get(idx);
    }

    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values, or null if there are no non-null values.
     */
    public static ${pt.primitive} max(${pt.primitive}... values) {
        if (values == null) {
            return ${pt.null};
        }

        return max(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the maximum.  Null values are excluded.
     *
     * @param values values.
     * @return maximum of non-null values, or null if there are no non-null values.
     */
    public static ${pt.primitive} max(${pt.boxed}[] values) {
        final long idx = indexOfMax(values);
        return idx == NULL_LONG ? ${pt.null} : values[LongSizedDataStructure.intSize("max",idx)];
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values, or null if there are no non-null values.
     */
    public static ${pt.primitive} min(${pt.vector} values) {
        final long idx = indexOfMin(values);
        return idx == NULL_LONG ? ${pt.null} : values.get(idx);
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values, or null if there are no non-null values.
     */
    public static ${pt.primitive} min(${pt.primitive}... values) {
        if (values == null) {
            return ${pt.null};
        }

        return min(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the minimum.  Null values are excluded.
     *
     * @param values values.
     * @return minimum of non-null values, or null if there are no non-null values.
     */
    public static ${pt.primitive} min(${pt.boxed}[] values) {
        final long idx = indexOfMin(values);
        return idx == NULL_LONG ? ${pt.null} : values[LongSizedDataStructure.intSize("min",idx)];
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static long indexOfMax(${pt.boxed}[] values) {
        return indexOfMax(unbox(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static long indexOfMax(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return indexOfMax(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static long indexOfMax(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        ${pt.primitive} val = ${pt.minValue};
        long index = NULL_LONG;
        long count = 0;
        long i = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                if (!isNull(c) && (c > val || (c == val && count == 0))) {
                    val = c;
                    index = i;
                    count++;
                }

                i++;
            }
        }

        return count == 0 ? NULL_LONG : index;
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static long indexOfMin(${pt.boxed}[] values) {
        return indexOfMin(unbox(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static long indexOfMin(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return indexOfMin(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static long indexOfMin(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        ${pt.primitive} val = ${pt.maxValue};
        long index = NULL_LONG;
        long count = 0;
        long i = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                if (!isNull(c) && (c < val || (c == val && count == 0) )) {
                    val = c;
                    index = i;
                    count++;
                }

                i++;
            }
        }

        return count == 0 ? NULL_LONG : index;
    }

    /**
     * Returns the median. {@code null} input values are ignored but {@code NaN} values will poison the computation,
     * and {@code NaN} will be returned
     *
     * @param values values.
     * @return median.
     */
    public static double median(${pt.boxed}[] values) {
        return median(unbox(values));
    }

    /**
     * Returns the median. {@code null} input values are ignored but {@code NaN} values will poison the computation,
     * and {@code NaN} will be returned
     *
     * @param values values.
     * @return median.
     */
    public static double median(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return median(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the median. {@code null} input values are ignored but {@code NaN} values will poison the computation,
     * and {@code NaN} will be returned
     *
     * @param values values.
     * @return median.
     */
    public static double median(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        int n = values.intSize("median");

        if (n == 0) {
            return NULL_DOUBLE;
        }

        ${pt.primitive}[] sorted = values.copyToArray();
        Arrays.sort(sorted);

        <#if pt.valueType.isFloat >
        if (isNaN(sorted[sorted.length - 1])) {
            return Double.NaN; // Any NaN will pollute the result and NaN always sorted to the end.
        }

        int nullStart = -1;
        int nullCount = 0;
        for (int i = 0; i < n; i++) {
            final ${pt.primitive} val = sorted[i];
            if (val > ${pt.null}) {
                break; // no more NULL possible
            }
            if (isNull(val)) {
                nullCount++;
                if (nullStart == -1) {
                    nullStart = i;
                }
            }
        }
        <#else>
        int nullCount = 0;
        for (int i = 0; i < n && isNull(sorted[i]); i++) {
            nullCount++;
        }
        </#if>

        if (nullCount == n) {
            return NULL_DOUBLE;
        }
        if (nullCount > 0) {
            n -= nullCount;
            final int medianIndex = n / 2;
            if (n % 2 == 0) {
            <#if pt.valueType.isFloat >
                final int idx1 = (medianIndex - 1) < nullStart ? (medianIndex - 1) : (medianIndex - 1) + nullCount;
                final int idx2 =  medianIndex < nullStart ? medianIndex : medianIndex + nullCount;
            <#else>
                final int idx1 = (medianIndex - 1) + nullCount;
                final int idx2 = medianIndex + nullCount;
            </#if>
                return 0.5 * (sorted[idx1] + sorted[idx2]);
            }
            <#if pt.valueType.isFloat >
            final int adjustedIndex = medianIndex < nullStart ? medianIndex : medianIndex + nullCount;
            <#else>
            final int adjustedIndex = medianIndex + nullCount;
            </#if>
            return sorted[adjustedIndex];
        }
        final int medianIndex = n / 2;
        if (n % 2 == 0) {
            return 0.5 * (sorted[medianIndex - 1] + sorted[medianIndex]);
        }
        return sorted[medianIndex];
    }

    /**
     * Returns the percentile. {@code null} input values are ignored but {@code NaN} values will poison the computation,
     * and {@code NaN} will be returned
     *
     * @param percentile percentile to compute.
     * @param values values.
     * @return percentile, or null value in the Deephaven convention if values is null or empty.
     */
    public static ${pt.primitive} percentile(double percentile, ${pt.primitive}... values) {
        if (values == null || values.length == 0) {
            return ${pt.null};
        }

        return percentile(percentile, new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the percentile. {@code null} input values are ignored but {@code NaN} values will poison the computation,
     * and {@code NaN} will be returned
     *
     * @param percentile percentile to compute.
     * @param values values.
     * @return percentile, or null value in the Deephaven convention if values is null or empty.
     */
    public static ${pt.primitive} percentile(double percentile, ${pt.vector} values) {
        if (values == null || values.isEmpty()) {
            return ${pt.null};
        }

        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Invalid percentile = " + percentile);
        }

        int n = values.intSize("percentile");

        ${pt.primitive}[] sorted = values.copyToArray();
        Arrays.sort(sorted);

        <#if pt.valueType.isFloat >
        if (isNaN(sorted[sorted.length - 1])) {
            return ${pt.boxed}.NaN; // Any NaN will pollute the result and NaN always sorted to the end.
        }
        int nullStart = -1;
        int nullCount = 0;
        for (int i = 0; i < n; i++) {
            final ${pt.primitive} val = sorted[i];
            if (val > ${pt.null}) {
                break; // no more NULL possible
            }
            if (isNull(val)) {
                nullCount++;
                if (nullStart == -1) {
                    nullStart = i;
                }
            }
        }
        <#else>
        int nullCount = 0;
        for (int i = 0; i < n && isNull(sorted[i]); i++) {
            nullCount++;
        }
        </#if>

        if (nullCount == n) {
            return ${pt.null};
        }
        if (nullCount > 0) {
            n -= nullCount;
            <#if pt.valueType.isFloat >
            final int idx = (int) Math.round(percentile * (n - 1));
            final int adjustedIndex = idx < nullStart ? idx : idx + nullCount;
            return sorted[adjustedIndex];
            <#else>
            int idx = (int) Math.round(percentile * (n - 1));
            return sorted[idx + nullCount];
            </#if>
        }
        int idx = (int) Math.round(percentile * (n - 1));
        return sorted[idx];
    }


    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.primitive}[] values0, ${pt2.primitive}[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new ${pt.vectorDirect}(values0), new ${pt2.vector}Direct(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.primitive}[] values0, ${pt2.vector} values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new ${pt.vectorDirect}(values0), values1);
    }

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.vector} values0, ${pt2.primitive}[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(values0, new ${pt2.vector}Direct(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.vector} values0, ${pt2.vector} values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        if (values0.size() != values1.size()) {
            throw new IllegalArgumentException("Input arrays are different lengths!");
        }

        double sum0 = 0;
        double sum1 = 0;
        double sum01 = 0;
        double count = 0;
        long nullCount = 0;

        try (
            final ${pt.vectorIterator} v0i = values0.iterator();
            final ${pt2.vectorIterator} v1i = values1.iterator()
        ) {
            while (v0i.hasNext()) {
                final ${pt.primitive} v0 = v0i.${pt.iteratorNext}();
                final ${pt2.primitive} v1 = v1i.${pt2.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(v0) || isInf(v0)) {
                    return Double.NaN;
                }
                </#if>
                <#if pt2.valueType.isFloat >
                if (isNaN(v1) || isInf(v1)) {
                    return Double.NaN;
                }
                </#if>

                if (!isNull(v0) && !isNull(v1)) {
                    sum0 += v0;
                    sum1 += v1;
                    sum01 += v0 * v1;
                    count++;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values0.size()) {
            return NULL_DOUBLE;
        }

        return sum01 / count - sum0 * sum1 / count / count;
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.primitive}[] values0, ${pt2.primitive}[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new ${pt.vectorDirect}(values0), new ${pt2.vector}Direct(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.primitive}[] values0, ${pt2.vector} values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new ${pt.vectorDirect}(values0), values1);
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.vector} values0, ${pt2.primitive}[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(values0, new ${pt2.vector}Direct(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.vector} values0, ${pt2.vector} values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        if (values0.size() != values1.size()) {
            throw new IllegalArgumentException("Input arrays are different lengths!");
        }

        double sum0 = 0;
        double sum0Sq = 0;
        double sum1 = 0;
        double sum1Sq = 0;
        double sum01 = 0;
        double count = 0;
        long nullCount = 0;

        try (
            final ${pt.vectorIterator} v0i = values0.iterator();
            final ${pt2.vectorIterator} v1i = values1.iterator()
        ) {
            while (v0i.hasNext()) {
                final ${pt.primitive} v0 = v0i.${pt.iteratorNext}();
                final ${pt2.primitive} v1 = v1i.${pt2.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(v0) || isInf(v0)) {
                    return Double.NaN;
                }
                </#if>
                <#if pt2.valueType.isFloat >
                if (isNaN(v1) || isInf(v1)) {
                    return Double.NaN;
                }
                </#if>

                if (!isNull(v0) && !isNull(v1)) {
                    sum0 += v0;
                    sum0Sq += v0 * v0;
                    sum1 += v1;
                    sum1Sq += v1 * v1;
                    sum01 += v0 * v1;
                    count++;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values0.size()) {
            return NULL_DOUBLE;
        }

        double cov = sum01 / count - sum0 * sum1 / count / count;
        double var0 = sum0Sq / count - sum0 * sum0 / count / count;
        double var1 = sum1Sq / count - sum1 * sum1 / count / count;

        return cov / Math.sqrt(var0 * var1);
    }

    </#if>
    </#list>


    /**
     * Returns the sum.  Null values are excluded.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double sum(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        long nullCount = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();

                if (isNaN(c) || isNaN(sum)) {
                    return Double.NaN;
                }

                if (!isNull(c)) {
                    sum += c;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        return sum;
    }
    <#else>
    public static long sum(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long sum = 0;
        long nullCount = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();

                if (!isNull(c)) {
                    sum += c;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_LONG;
        }

        return sum;
    }
    </#if>

    /**
     * Returns the sum.  Null values are excluded.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double sum(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return sum(new ${pt.vectorDirect}(values));
    }
    <#else>
    public static long sum(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return sum(new ${pt.vectorDirect}(values));
    }
    </#if>

    /**
     * Returns the product.  Null values are excluded.
     *
     * @param values values.
     * @return product of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double product(${pt.vector} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double prod = 1;
        int count = 0;
        boolean hasZero = false;
        boolean hasInf = false;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();

                if (isNaN(c)) {
                    return Double.NaN;
                } else if (Double.isInfinite(c)) {
                    if (hasZero) {
                        return Double.NaN;
                    }
                    hasInf = true;
                } else if (c == 0) {
                    if (hasInf) {
                        return Double.NaN;
                    }
                    hasZero = true;
                }

                if (!isNull(c)) {
                    count++;
                    prod *= c;
                }
            }
        }

        if (count == 0) {
            return NULL_DOUBLE;
        }

        return hasZero ? 0 : prod;
    }
    <#else>
    public static long product(${pt.vector} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long prod = 1;
        int count = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();

                if (c == 0) {
                    return 0;
                }

                if (!isNull(c)) {
                    count++;
                    prod *= c;
                }
            }
        }

        if (count == 0) {
            return NULL_LONG;
        }

        return prod;
    }
    </#if>

    /**
     * Returns the product.  Null values are excluded.
     *
     * @param values values.
     * @return product of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double product(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return product(new ${pt.vectorDirect}(values));
    }
    <#else>
    public static long product(${pt.primitive}... values) {
        if (values == null) {
            return NULL_LONG;
        }

        return product(new ${pt.vectorDirect}(values));
    }
    </#if>

    /**
     * Returns the differences between elements in the input vector separated by a stride.
     * A stride of k returns v(i)=e(i+k)-e(i), where v(i) is the ith computed value, and e(i) is the ith input value.
     * A stride of -k returns v(i)=e(i-k)-e(i), where v(i) is the ith computed value, and e(i) is the ith input value.
     * The result has the same length as the input vector.
     * Differences off the end of the input vector are the null value.
     *
     * @param stride number of elements separating the elements to be differenced.
     * @param values input vector.
     * @return a vector containing the differences between elements.
     */
    public static ${pt.primitive}[] diff(int stride, ${pt.boxed}[] values) {
        return diff(stride, unbox(values));
    }

    /**
     * Returns the differences between elements in the input vector separated by a stride.
     * A stride of k returns v(i)=e(i+k)-e(i), where v(i) is the ith computed value e(i) is the ith input value.
     * A stride of -k returns v(i)=e(i-k)-e(i), where v(i) is the ith computed value e(i) is the ith input value.
     * The result has the same length as the input vector.
     * Differences off the end of the input vector are the null value.
     *
     * @param stride number of elements separating the elements to be differenced.
     * @param values input vector.
     * @return a vector containing the differences between elements.
     */
    public static ${pt.primitive}[] diff(int stride, ${pt.primitive}... values) {
        return diff(stride, new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the differences between elements in the input vector separated by a stride.
     * A stride of k returns v(i)=e(i+k)-e(i), where v(i) is the ith computed value e(i) is the ith input value.
     * A stride of -k returns v(i)=e(i-k)-e(i), where v(i) is the ith computed value e(i) is the ith input value.
     * The result has the same length as the input vector.
     * Differences off the end of the input vector are the null value.
     *
     * @param stride number of elements separating the elements to be differenced.
     * @param values input vector.
     * @return a vector containing the differences between elements.
     */
    public static ${pt.primitive}[] diff(int stride, ${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new ${pt.primitive}[0];
        }

        final int n = values.intSize("diff");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        for (int i = 0; i < n; i++) {
            ${pt.primitive} v1 = values.get(i);
            ${pt.primitive} v2 = values.get(i + stride);

            if (isNull(v1) || isNull(v2)) {
                result[i] = ${pt.null};
            } else {
                result[i] = (${pt.primitive})(v2 - v1);
            }
        }

        return result;
    }

    /**
     * Returns the cumulative minimum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative min of non-null values.
     */
    public static ${pt.primitive}[] cummin(${pt.boxed}[] values) {
        return cummin(unbox(values));
    }

    /**
     * Returns the cumulative minimum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative min of non-null values.
     */
    public static ${pt.primitive}[] cummin(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return cummin(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the cumulative minimum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative min of non-null values.
     */
    public static ${pt.primitive}[] cummin(${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new ${pt.primitive}[0];
        }

        final int n = values.intSize("cummin");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            result[0] = vi.${pt.iteratorNext}();
            int i = 1;

            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();

                if (isNull(result[i - 1])) {
                    result[i] = v;
                } else if (isNull(v)) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = (${pt.primitive})Math.min(result[i - 1],  v);
                }

                i++;
            }
        }

        return result;
    }

    /**
     * Returns the cumulative maximum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative max of non-null values.
     */
    public static ${pt.primitive}[] cummax(${pt.boxed}[] values) {
        return cummax(unbox(values));
    }

    /**
     * Returns the cumulative maximum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative max of non-null values.
     */
    public static ${pt.primitive}[] cummax(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return cummax(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the cumulative maximum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative max of non-null values.
     */
    public static ${pt.primitive}[] cummax(${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new ${pt.primitive}[0];
        }

        final int n = values.intSize("cummax");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            result[0] = vi.${pt.iteratorNext}();
            int i = 1;

            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();

                if (isNull(result[i - 1])) {
                    result[i] = v;
                } else if (isNull(v)) {
                    result[i] = result[i - 1];
                } else {
                    result[i] = (${pt.primitive})Math.max(result[i - 1], v);
                }

                i++;
            }
        }

        return result;
    }

   /**
     * Returns the cumulative sum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double[] cumsum(${pt.boxed}[] values) {
        return cumsum(unbox(values));
    }
   <#else>
    public static long[] cumsum(${pt.boxed}[] values) {
        return cumsum(unbox(values));
    }
   </#if>

    /**
     * Returns the cumulative sum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double[] cumsum(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return cumsum(new ${pt.vectorDirect}(values));
    }
   <#else>
    public static long[] cumsum(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return cumsum(new ${pt.vectorDirect}(values));
    }
   </#if>

    /**
     * Returns the cumulative sum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double[] cumsum(${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new double[0];
        }

        final int n = values.intSize("cumsum");
        final double[] result = new double[n];

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            final ${pt.primitive} v0 = vi.${pt.iteratorNext}();
            result[0] = isNull(v0) ? NULL_DOUBLE : v0;
            int i = 1;
    
            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();

                if (isNaN(v) || isNaN(result[i - 1])) {
                    Arrays.fill(result, i, n, Double.NaN);
                    return result;
                } else if (isNull(v)) {
                    result[i] = result[i - 1];
                } else if (isNull(result[i - 1])) {
                    result[i] = v;
                } else {
                    result[i] = result[i - 1] + v;
                }
    
                i++;
            }
        }

        return result;
    }
    <#else>
    public static long[] cumsum(${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new long[0];
        }

        final int n = values.intSize("cumsum");
        final long[] result = new long[n];

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            final ${pt.primitive} v0 = vi.${pt.iteratorNext}();
            result[0] = isNull(v0) ? NULL_LONG : v0;
            int i = 1;

            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();

                if (isNull(v)) {
                    result[i] = result[i - 1];
                } else if (isNull(result[i - 1])) {
                    result[i] = v;
                } else {
                    result[i] = result[i - 1] + v;
                }

                i++;
            }
        }

        return result;
    }
    </#if>

    /**
     * Returns the cumulative product.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double[] cumprod(${pt.boxed}[] values) {
        return cumprod(unbox(values));
    }
    <#else>
    public static long[] cumprod(${pt.boxed}[] values) {
        return cumprod(unbox(values));
    }
    </#if>

    /**
     * Returns the cumulative product.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double[] cumprod(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return cumprod(new ${pt.vectorDirect}(values));
    }
    <#else>
    public static long[] cumprod(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return cumprod(new ${pt.vectorDirect}(values));
    }
    </#if>

    /**
     * Returns the cumulative product.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    <#if pt.valueType.isFloat >
    public static double[] cumprod(${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new double[0];
        }

        final int n = values.intSize("cumprod");
        double[] result = new double[n];

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            final ${pt.primitive} v0 = vi.${pt.iteratorNext}();
            result[0] = isNull(v0) ? NULL_DOUBLE : v0;
            int i = 1;
    
            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();

                if (isNaN(v) || isNaN(result[i - 1])) {
                    Arrays.fill(result, i, n, Double.NaN);
                    return result;
                } else if (isNull(v)) {
                    result[i] = result[i - 1];
                } else if (isNull(result[i - 1])) {
                    result[i] = v;
                } else {
                    result[i] = result[i - 1] * v;
                }
    
                i++;
            }
        }

        return result;
    }
    <#else>
    public static long[] cumprod(${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new long[0];
        }

        final int n = values.intSize("cumprod");
        long[] result = new long[n];

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            final ${pt.primitive} v0 = vi.${pt.iteratorNext}();
            result[0] = isNull(v0) ? NULL_LONG : v0;
            int i = 1;

            while (vi.hasNext()) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();

                if (isNull(v)) {
                    result[i] = result[i - 1];
                } else if (isNull(result[i - 1])) {
                    result[i] = v;
                } else {
                    result[i] = result[i - 1] * v;
                }

                i++;
            }
        }

        return result;
    }
    </#if>

    /**
     * Returns the absolute value.
     *
     * @param value value.
     * @return absolute value.
     */
    public static ${pt.primitive} abs(${pt.primitive} value) {
        if (isNull(value)) {
            return ${pt.null};
        }

        return (${pt.primitive}) Math.abs(value);
    }

    /**
     * Returns the arc cosine.
     *
     * @param value value.
     * @return arc cosine.
     */
    public static double acos(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double asin(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double atan(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double ceil(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double cos(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double exp(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double floor(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double log(${pt.primitive} value) {
        if (isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.log(value);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the value of the first argument raised to the second argument.
     *
     * @param   a   the base.
     * @param   b   the exponent.
     * @return {@code a} raised to the {@code b} power.
     */
    public static double pow(${pt.primitive} a, ${pt2.primitive} b) {
        if (isNull(a) || isNull(b)) {
            return NULL_DOUBLE;
        }

        return Math.pow(a, b);
    }

    </#if>
    </#list>

    /**
     * Returns the integer closest to the input value.
     *
     * @param value value.
     * @return integer closes to the input value.
     */
    public static double rint(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static long round(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static int signum(${pt.primitive} value) {
        if (isNull(value)) {
            return NULL_INT;
        }

        return Integer.signum((int)value);
    }

    /**
     * Returns the sine.
     *
     * @param value value.
     * @return sine.
     */
    public static double sin(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double sqrt(${pt.primitive} value) {
        if (isNull(value)) {
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
    public static double tan(${pt.primitive} value) {
        if (isNull(value)) {
            return NULL_DOUBLE;
        }

        return Math.tan(value);
    }


    <#if pt.valueType.isFloat >

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * The lower bound of the bin containing the value is equal to <code>interval * floor(value / interval)</code>.
     *
     * @param value value.
     * @param interval bin width.
     * @return lower bound of the bin containing the value.
     */
    public static ${pt.primitive} lowerBin(${pt.primitive} value, ${pt.primitive} interval) {
        if (value == ${pt.null} || interval == ${pt.null}) {
            return ${pt.null};
        }

        if ( interval <= 0 ) {
            throw new IllegalArgumentException("Interval is not positive: " + interval);
        }

        return (${pt.primitive}) (interval * Math.floor(value / interval));
    }

    <#else>

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * The lower bound of the bin containing the value is equal to <code>interval * floor(value / interval)</code>.
     *
     * @param value value.
     * @param interval bin width.
     * @return lower bound of the bin containing the value.
     */
   public static ${pt.primitive} lowerBin(${pt.primitive} value, ${pt.primitive} interval) {
        if (value == ${pt.null} || interval == ${pt.null}) {
            return ${pt.null};
        }

        if ( interval <= 0 ) {
            throw new IllegalArgumentException("Interval is not positive: " + interval);
        }

        final long d = ((long)value) / ((long)interval);
        final long m = ((long)value) % ((long)interval);
        final long r = (m != 0 && value < 0) ? d - 1 : d;
        return (${pt.primitive}) (interval * r);
    }

    </#if>

    /**
     * Returns the lower bound of the bin containing the value.
     *
     * The lower bound of the bin containing the value is equal to <code>interval * floor((value-offset) / interval) + offset</code>.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return lower bound of the bin containing the value.
     */
    public static ${pt.primitive} lowerBin(${pt.primitive} value, ${pt.primitive} interval, ${pt.primitive} offset) {
        if (value == ${pt.null} || interval == ${pt.null}) {
            return ${pt.null};
        }

        return (${pt.primitive})(lowerBin((${pt.primitive})(value-offset),interval) + offset);
    }

    <#if pt.valueType.isFloat >

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * The upper bound of the bin containing the value is equal to <code>interval * ceil(value / interval)</code>.
     *
     * @param value value.
     * @param interval bin width.
     * @return upper bound of the bin containing the value.
     */
    public static ${pt.primitive} upperBin(${pt.primitive} value, ${pt.primitive} interval) {
        if (value == ${pt.null} || interval == ${pt.null}) {
            return ${pt.null};
        }

        if ( interval <= 0 ) {
            throw new IllegalArgumentException("Interval is not positive: " + interval);
        }

        return (${pt.primitive}) (interval * Math.ceil(value / interval));
    }

    <#else>

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * The upper bound of the bin containing the value is equal to <code>interval * ceil(value / interval)</code>.
     *
     * @param value value.
     * @param interval bin width.
     * @return upper bound of the bin containing the value.
     */
    public static ${pt.primitive} upperBin(${pt.primitive} value, ${pt.primitive} interval) {
        if (value == ${pt.null} || interval == ${pt.null}) {
            return ${pt.null};
        }

        if ( interval <= 0 ) {
            throw new IllegalArgumentException("Interval is not positive: " + interval);
        }

        final long r = ((long)value) / ((long)interval) + (value % interval > 0 ? 1 : 0);
        return (${pt.primitive}) (interval * r);
    }

    </#if>

    /**
     * Returns the upper bound of the bin containing the value.
     *
     * The upper bound of the bin containing the value is equal to <code>interval * ceil((value-offset) / interval) + offset</code>.
     *
     * @param value value.
     * @param interval bin width.
     * @param offset interval offset
     * @return upper bound of the bin containing the value.
     */
    public static ${pt.primitive} upperBin(${pt.primitive} value, ${pt.primitive} interval, ${pt.primitive} offset) {
        if (value == ${pt.null} || interval == ${pt.null}) {
            return ${pt.null};
        }

        return (${pt.primitive})(upperBin((${pt.primitive})(value-offset),interval) + offset);
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
    public static ${pt.primitive} clamp(${pt.primitive} value, ${pt.primitive} min, ${pt.primitive} max) {
        Require.leq(min, "min", max, "max");

        if (isNull(value)) {
            return ${pt.null};
        }

        if (value < min) {
            return min;
        } else if (value > max) {
            return max;
        } else {
            return value;
        }
    }


    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
   <#if pt.valueType.isInteger && pt2.valueType.isInteger >
   public static long wsum(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_LONG;
        }

        return wsum(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
   }
   <#else>
   public static double wsum(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
    }
    </#if>

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
   <#if pt.valueType.isInteger && pt2.valueType.isInteger >
    public static long wsum(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_LONG;
        }

        return wsum(new ${pt.vectorDirect}(values), weights);
    }
   <#else>
    public static double wsum(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new ${pt.vectorDirect}(values), weights);
    }
   </#if>

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
   <#if pt.valueType.isInteger && pt2.valueType.isInteger >
    public static long wsum(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_LONG;
        }

        return wsum(values, new ${pt2.vector}Direct(weights));
    }
    <#else>
    public static double wsum(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new ${pt2.vector}Direct(weights));
    }
    </#if>

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
   <#if pt.valueType.isInteger && pt2.valueType.isInteger >
    public static long wsum(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_LONG;
        }

        final long n = values.size();

        if (n != weights.size()) {
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        long vsum = 0;
        long nullCount = 0;

        try (
            final ${pt.vectorIterator} vi = values.iterator();
            final ${pt2.vectorIterator} wi = weights.iterator()
        ) {
            while (vi.hasNext()) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                final ${pt2.primitive} w = wi.${pt2.iteratorNext}();

                if (!isNull(c) && !isNull(w)) {
                    vsum += c * (long) w;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_LONG;
        }

        return vsum;
    }
    <#else>
    public static double wsum(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final long n = values.size();

        if (n != weights.size()) {
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        double vsum = 0;
        long nullCount = 0;

        try (
            final ${pt.vectorIterator} vi = values.iterator();
            final ${pt2.vectorIterator} wi = weights.iterator()
        ) {
            while (vi.hasNext()) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                final ${pt2.primitive} w = wi.${pt2.iteratorNext}();

                if (isNaN(vsum)) {
                    return Double.NaN;
                }

               <#if pt.valueType.isFloat >
                if (isNaN(c)) {
                    return Double.NaN;
                }
               </#if>

               <#if pt2.valueType.isFloat >
                if (isNaN(w)) {
                    return Double.NaN;
                }
               </#if>

                if (!isNull(c) && !isNull(w)) {
                   <#if pt.valueType.isFloat >
                    vsum += (double) c * w;
                   <#else>
                    vsum += c * (double) w;
                   </#if>
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        return vsum;
    }
    </#if>

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new ${pt.vectorDirect}(values), new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.primitive}[] values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new ${pt.vectorDirect}(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.vector} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new ${pt2.vector}Direct(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.vector} values, ${pt2.vector} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final long n = values.size();

        if (n != weights.size()) {
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        double vsum = 0;
        double wsum = 0;
        long nullCount = 0;

        try (
            final ${pt.vectorIterator} vi = values.iterator();
            final ${pt2.vectorIterator} wi = weights.iterator()
        ) {
            while (vi.hasNext()) {
                final ${pt.primitive} c = vi.${pt.iteratorNext}();
                final ${pt2.primitive} w = wi.${pt2.iteratorNext}();
                <#if pt.valueType.isFloat >
                if (isNaN(c)) {
                    return Double.NaN;
                }
                </#if>
                <#if pt2.valueType.isFloat >
                if (isNaN(w)) {
                    return Double.NaN;
                }
                </#if>
                if (!isNull(c) && !isNull(w)) {
                    vsum += c * w;
                    wsum += w;
                } else {
                    nullCount++;
                }
            }
        }

        if (nullCount == values.size()) {
            return NULL_DOUBLE;
        }

        return vsum / wsum;
    }

    </#if>
    </#list>


    /**
     * Returns a sequence of values.
     *
     * @param start starting value.
     * @param end terminal value.
     * @param step step size.
     * @return sequence of values from start to end.
     */
    public static ${pt.primitive}[] sequence(${pt.primitive} start, ${pt.primitive} end, ${pt.primitive} step) {
        if (step == 0) {
            return new ${pt.primitive}[0];
        }

        final int n = (int)((end-start)/step);

        if (n < 0) {
            return new ${pt.primitive}[0];
        }

        final ${pt.primitive}[] result = new ${pt.primitive}[n+1];

        for (int i=0; i<=n; i++) {
            result[i] = (${pt.primitive})(start + i*step);
        }

        return result;
    }


    <#if pt.valueType.isFloat >

    /**
     * Returns {@code true} if the value is NaN and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is NaN and {@code false} otherwise.
     */
    static public boolean isNaN(${pt.boxed} value) {
        return value != null && ${pt.boxed}.isNaN(value);
    }

    /**
     * Returns {@code true} if the value is NaN and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is NaN and {@code false} otherwise.
     */
    static public boolean isNaN(${pt.primitive} value) {
        return ${pt.boxed}.isNaN(value);
    }

    /**
     * Returns {@code true} if the value is infinite and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is infinite and {@code false} otherwise.
     */
    static public boolean isInf(${pt.boxed} value) {
        return value != null && ${pt.boxed}.isInfinite(value);
    }

    /**
     * Returns {@code true} if the value is infinite and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is infinite and {@code false} otherwise.
     */
    static public boolean isInf(${pt.primitive} value) {
        return ${pt.boxed}.isInfinite(value);
    }

    /**
     * Returns {@code true} if the value is finite, where "finite" is defined as not infinite, not NaN, and not null.
     *
     * @param value value.
     * @return {@code true} if the value is not infinite, NaN, nor null; {@code false} otherwise
     */
    static public boolean isFinite(${pt.boxed} value) {
        return isFinite(castDouble(value));
    }

    /**
     * Returns {@code true} if the value is finite, where "finite" is defined as not infinite, not NaN, and not null.
     *
     * @param value value.
     * @return {@code true} if the value is not infinite, NaN, nor null; {@code false} otherwise
     */
    static public boolean isFinite(${pt.primitive} value) {
        return ${pt.boxed}.isFinite(value) && !isNull(value);
    }

    /**
     * Returns {@code true} if the values contains any non-finite value, where "finite" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isFinite(${pt.primitive}) finite}; {@code false} otherwise.
     * @see #isFinite(${pt.primitive})
     */
    static public boolean containsNonFinite(${pt.boxed}[] values) {
        return containsNonFinite(unbox(values));
    }

    /**
     * Returns {@code true} if the values contains any non-finite value, where "finite" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isFinite(${pt.primitive}) finite}; {@code false} otherwise.
     * @see #isFinite(${pt.primitive})
     */
    static public boolean containsNonFinite(${pt.primitive}... values) {
        for (${pt.primitive} v1 : values) {
            if (!isFinite(v1)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Replaces values that are NaN with a specified value.
     *
     * @param value value.
     * @param replacement replacement to use when value is NaN.
     * @return value, if value is not NaN, replacement otherwise.
     */
    static public ${pt.primitive} replaceIfNaN(${pt.primitive} value, ${pt.primitive} replacement) {
        if (isNaN(value)) {
            return replacement;
        } else {
            return value;
        }
    }

    /**
     * Replaces values that are NaN with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is NaN.
     * @return array containing value, if value is not NaN, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNaN(${pt.primitive}[] values, ${pt.primitive} replacement) {
        return replaceIfNaN(new ${pt.vectorDirect}(values), replacement);
    }

    /**
     * Replaces values that are NaN with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is NaN.
     * @return array containing value, if value is not NaN, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNaN(${pt.vector} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNaN");
        ${pt.primitive}[] result = new ${pt.primitive}[n];
        int i = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();
                result[i] = replaceIfNaN(v, replacement);
                i++;
            }
        }

        return result;
    }

    /**
     * Replaces values that are NaN or null according to Deephaven convention with a specified value.
     *
     * @param value value.
     * @param replacement replacement to use when value is NaN or null according to Deephaven convention.
     * @return value, if value is neither NaN nor null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive} replaceIfNullNaN(${pt.primitive} value, ${pt.primitive} replacement) {
        if (isNaN(value) || isNull(value)) {
            return replacement;
        } else {
            return value;
        }
    }

    /**
     * Replaces values that are NaN or null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is NaN or null according to Deephaven convention.
     * @return array containing value, if value is neither NaN nor null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNullNaN(${pt.primitive}[] values, ${pt.primitive} replacement) {
        return replaceIfNullNaN(new ${pt.vectorDirect}(values), replacement);
    }

    /**
     * Replaces values that are NaN or null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is NaN or null according to Deephaven convention.
     * @return array containing value, if value is neither NaN nor null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNullNaN(${pt.vector} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNullNaN");
        ${pt.primitive}[] result = new ${pt.primitive}[n];
        int i = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();
                result[i] = replaceIfNullNaN(v, replacement);
                i++;
            }
        }

        return result;
    }

    /**
     * Replaces values that are not finite according to Deephaven convention with a specified value.
     *
     * @param value value.
     * @param replacement replacement to use when value is not finite according to Deephaven convention.
     * @return value, if value is finite according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive} replaceIfNonFinite(${pt.primitive} value, ${pt.primitive} replacement) {
        return isFinite(value) ? value : replacement;
    }

    /**
     * Replaces values that are not finite according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is not finite according to Deephaven convention.
     * @return array containing value, if value is finite according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNonFinite(${pt.primitive}[] values, ${pt.primitive} replacement) {
        return replaceIfNonFinite(new ${pt.vectorDirect}(values), replacement);
    }

    /**
     * Replaces values that are not finite according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is not finite according to Deephaven convention.
     * @return array containing value, if value is finite according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNonFinite(${pt.vector} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNonFinite");
        ${pt.primitive}[] result = new ${pt.primitive}[n];
        int i = 0;

        try ( final ${pt.vectorIterator} vi = values.iterator() ) {
            while ( vi.hasNext() ) {
                final ${pt.primitive} v = vi.${pt.iteratorNext}();
                result[i] = replaceIfNonFinite(v, replacement);
                i++;
            }
        }

        return result;
    }

    <#else>

    /**
     * Returns {@code true} if the value is NaN and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is NaN and {@code false} otherwise.
     */
    static public boolean isNaN(${pt.boxed} value) {
        return false;
    }

    /**
     * Returns {@code true} if the value is NaN and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is NaN and {@code false} otherwise.
     */
    static public boolean isNaN(${pt.primitive} value) {
        return false;
    }

    /**
     * Returns {@code true} if the value is infinite and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is infinite and {@code false} otherwise.
     */
    static public boolean isInf(${pt.boxed} value) {
        return false;
    }

    /**
     * Returns {@code true} if the value is infinite and {@code false} otherwise.
     *
     * @param value value.
     * @return {@code true} if the value is infinite and {@code false} otherwise.
     */
    static public boolean isInf(${pt.primitive} value) {
        return false;
    }

    /**
     * Returns {@code true} if the value is finite, where "finite" is defined as not infinite, not NaN, and not null.
     *
     * @param value value.
     * @return {@code true} if the value is not infinite, NaN, nor null; {@code false} otherwise
     */
    static public boolean isFinite(${pt.boxed} value) {
        return !isNull(value);
    }

    /**
     * Returns {@code true} if the value is finite, where "finite" is defined as not infinite, not NaN, and not null.
     *
     * @param value value.
     * @return {@code true} if the value is not infinite, NaN, nor null; {@code false} otherwise
     */
    static public boolean isFinite(${pt.primitive} value) {
        return !isNull(value);
    }

    /**
     * Returns {@code true} if the values contains any non-finite value, where "finite" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isFinite(${pt.primitive}) finite}; {@code false} otherwise.
     * @see #isFinite(${pt.primitive})
     */
    static public boolean containsNonFinite(${pt.boxed}[] values) {
        for (${pt.boxed} v1 : values) {
            if (isNull(v1)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if the values contains any non-finite value, where "finite" is defined as
     * not infinite, not NaN, and not null.
     *
     * @param values values.
     * @return {@code true} if any value is not {@link #isFinite(${pt.primitive}) finite}; {@code false} otherwise.
     * @see #isFinite(${pt.primitive})
     */
    static public boolean containsNonFinite(${pt.primitive}... values) {
        for (${pt.primitive} v1 : values) {
            if (isNull(v1)) {
                return true;
            }
        }

        return false;
    }


    </#if>


    /**
     * Compares two specified values.  Deephaven null values are less than normal numbers which are less than NaN values.
     *
     * @param v1 the first value to compare.
     * @param v2 the second value to compare.
     * @returns the value 0 if v1 is numerically equal to v2; a value of less than 0 if v1 is numerically less than v2;
     *      and a value greater than 0 if v1 is numerically greater than v2.
     *      Deephaven null values are less than normal numbers which are less than NaN values.
     *      Unlike standard Java, Deephaven treats NaN values as ordered. In particular two NaN values will compare
     *      equal to each other, and a NaN value will compare greater than any other value.
     */
    static public int compare(${pt.primitive} v1, ${pt.primitive} v2) {
        final boolean isNull1 = isNull(v1);
        final boolean isNull2 = isNull(v2);

        if (isNull1 && isNull2) {
            return 0;
        } else if (isNull1) {
            return -1;
        } else if (isNull2) {
            return 1;
        }

        final boolean isNaN1 = isNaN(v1);
        final boolean isNaN2 = isNaN(v2);

        // NaN is considered the greatest
        if (isNaN1 && isNaN2) {
            return 0;
        } else if (isNaN1) {
            return 1;
        } else if (isNaN2) {
            return -1;
        }

        return ${pt.boxed}.compare(v1, v2);
    }


    /**
     * Compares two specified values.  Deephaven null values are less than normal numbers which are less than NaN values.
     *
     * @param v1 the first value to compare.
     * @param v2 the second value to compare.
     * @returns the value 0 if v1 is numerically equal to v2; a value of less than 0 if v1 is numerically less than v2;
     *      and a value greater than 0 if v1 is numerically greater than v2.
     *      Deephaven null values are less than normal numbers which are less than NaN values.
     *      Unlike standard Java, Deephaven treats NaN values as ordered. In particular two NaN values will compare
     *      equal to each other, and a NaN value will compare greater than any other value.
     */
    static public int compare(${pt.boxed} v1, ${pt.boxed} v2) {
        return compare(v1 == null ? ${pt.null} : v1, v2 == null ? ${pt.null} : v2);
    }

    /**
     * Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta).
     * This method computes the phase theta by computing an arc tangent of y/x in the range of -pi to pi.
     * Special cases:
     * <ul>
     *   <li>If either argument is NaN, then the result is NaN.</li>
     *   <li>If the first argument is positive zero and the second argument is positive, or the first argument is positive and finite and the second argument is positive infinity, then the result is positive zero.</li>
     *   <li>If the first argument is negative zero and the second argument is positive, or the first argument is negative and finite and the second argument is positive infinity, then the result is negative zero.</li>
     *   <li>If the first argument is positive zero and the second argument is negative, or the first argument is positive and finite and the second argument is negative infinity, then the result is the double value closest to pi.</li>
     *   <li>If the first argument is negative zero and the second argument is negative, or the first argument is negative and finite and the second argument is negative infinity, then the result is the double value closest to -pi.</li>
     *   <li>If the first argument is positive and the second argument is positive zero or negative zero, or the first argument is positive infinity and the second argument is finite, then the result is the double value closest to pi/2.</li>
     *   <li>If the first argument is negative and the second argument is positive zero or negative zero, or the first argument is negative infinity and the second argument is finite, then the result is the double value closest to -pi/2.</li>
     *   <li>If both arguments are positive infinity, then the result is the double value closest to pi/4.</li>
     *   <li>If the first argument is positive infinity and the second argument is negative infinity, then the result is the double value closest to 3*pi/4.</li>
     *   <li>If the first argument is negative infinity and the second argument is positive infinity, then the result is the double value closest to -pi/4.</li>
     *   <li>If both arguments are negative infinity, then the result is the double value closest to -3*pi/4.</li>
     * </ul>
     * The computed result must be within 2 ulps of the exact result. Results must be semi-monotonic.
     *
     * @param y the ordinate coordinate
     * @param x the abscissa coordinate
     * @return the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in
     *    Cartesian coordinates.  If either value is null, returns null.
     */
    static public double atan2(${pt.primitive} y, ${pt.primitive} x) {
        if (isNull(x) || isNull(y)) {
            return NULL_DOUBLE;
        }

        return Math.atan2(y, x);
    }

    /**
     * Returns the cube root of a value.
     *
     * @param x the value
     * @return the cube root of the value.  If the value is null, returns null.
     */
    static public double cbrt(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.cbrt(x);
    }

    /**
     * Returns the hyperbolic cosine.
     *
     * @param x the value
     * @return the hyperbolic cosine of the value.  If the value is null, returns null.
     */
    static public double cosh(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.cosh(x);
    }

    /**
     * Returns e^x - 1.
     *
     * @param x the value
     * @return e^x-1.  If the value is null, returns null.
     */
     static public double expm1(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.expm1(x);
     }

    /**
     * Returns the hypotenuse of a right-angled triangle, sqrt(x^2 + y^2), without intermediate overflow or underflow.
     *
     * @param x the first value.
     * @param y the second value.
     * @return the hypotenuse of a right-angled triangle.  If either value is null, returns null.
     */
    static public double hypot(${pt.primitive} x, ${pt.primitive} y) {
        if (isNull(x) || isNull(y)) {
            return NULL_DOUBLE;
        }

        return Math.hypot(x, y);
    }

    /**
     * Returns the base 10 logarithm of a value.
     *
     * @param x the value.
     * @return the base 10 logarithm of the value.  If the value is null, returns null.
     */
     static public double log10(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.log10(x);
     }

     /**
      * Returns the natural logarithm of the sum of the argument and 1.
      *
      * @param x the value.
      * @return the natural logarithm of the sum of the argument and 1.  If the value is null, returns null.
      */
     static public double log1p(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.log1p(x);
     }

     <#if pt.valueType.isFloat >
     /**
      * Returns x × 2^scaleFactor rounded as if performed by a single correctly rounded floating-point multiply to a
      * member of the ${pt.primitive} value set.
      *
      * @param x the value.
      * @param scaleFactor the scale factor.
      * @return x × 2scaleFactor rounded as if performed by a single correctly rounded floating-point multiply to a
      *     member of the double value set.  If the either value is null, returns null.
      */
     static public ${pt.primitive} scalb(${pt.primitive} x, int scaleFactor) {
        if (isNull(x) || isNull(scaleFactor)) {
            return ${pt.null};
        }

        return Math.scalb(x, scaleFactor);
     }
     </#if>

     /**
      * Returns the hyperbolic sine of a value.
      *
      * @param x the value
      * @return the hyperbolic sine of the value.  If the value is null, returns null.
      */
     static public double sinh(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.sinh(x);
     }

     /**
      * Returns the hyperbolic tangent of a value.
      *
      * @param x the value
      * @return the hyperbolic tangent of the value.  If the value is null, returns null.
      */
     static public double tanh(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.tanh(x);
     }

    /**
     * Returns the first argument with the sign of the second argument.
     *
     * @param magnitude the value to return
     * @param sign the sign for the return value
     * @return the value with the magnitude of the first argument and the sign of the second argument.
     *    If either value is null, returns null.
     */
    static public ${pt.primitive} copySign(${pt.primitive} magnitude, ${pt.primitive} sign) {
        if (isNull(magnitude) || isNull(sign)) {
            return ${pt.null};
        }

        <#if pt.valueType.isFloat >
        return Math.copySign(magnitude, sign);
        <#else>
        return (${pt.primitive}) ((magnitude < 0 ? -magnitude : magnitude) * (sign < 0 ? -1 : 1));
        </#if>
    }

    <#if pt.valueType.isInteger>
    /**
     * Returns the sum of its arguments, throwing an exception if the result overflows.
     *
     * @param x the first value.
     * @param y the second value.
     * @return the result of adding the arguments.  If either value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public ${pt.primitive} addExact(${pt.primitive} x, ${pt.primitive} y) {
        if (isNull(x) || isNull(y)) {
            return ${pt.null};
        }

        <#if pt.primitive == "byte" || pt.primitive == "short">
        int val = Math.addExact(x, y);

        if ( val > ${pt.maxValue} || val < ${pt.minValue} || isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x + " + " + y);
        }

        return (${pt.primitive}) val;
        <#else>
        ${pt.primitive} val = Math.addExact(x, y);

        if ( isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x + " + " + y);
        }

        return val;
        </#if>
    }
    </#if>

    <#if pt.valueType.isInteger>
    /**
     * Returns the difference of its arguments, throwing an exception if the result overflows.
     *
     * @param x the first value.
     * @param y the second value.
     * @return the result of subtracting the arguments.  If either value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public ${pt.primitive} subtractExact(${pt.primitive} x, ${pt.primitive} y) {
        if (isNull(x) || isNull(y)) {
            return ${pt.null};
        }

        <#if pt.primitive == "byte" || pt.primitive == "short">
        int val = Math.subtractExact(x, y);

        if ( val > ${pt.maxValue} || val < ${pt.minValue} || isNull(val)) {
            throw new ArithmeticException("Overflow: " + x + " - " + y);
        }

        return (${pt.primitive}) val;
        <#else>
        ${pt.primitive} val = Math.subtractExact(x, y);

        if ( isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x + " - " + y);
        }

        return val;
        </#if>
    }
    </#if>

     <#if pt.valueType.isInteger>
     /**
      * Returns the product of the arguments, throwing an exception if the result overflows.
      *
      * @param x the first value.
      * @param y the second value.
      * @return the result of multiplying the arguments.  If either value is null, returns null.
      * @throws ArithmeticException if the result overflows.
      */
     static public ${pt.primitive} multiplyExact(${pt.primitive} x, ${pt.primitive} y) {
         if (isNull(x) || isNull(y)) {
             return ${pt.null};
         }

         <#if pt.primitive == "byte" || pt.primitive == "short">
         int val = Math.multiplyExact(x, y);

         if ( val > ${pt.maxValue} || val < ${pt.minValue} || isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x + " * " + y);
         }

         return (${pt.primitive}) val;
         <#else>
        ${pt.primitive} val = Math.multiplyExact(x, y);

        if ( isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x + " * " + y);
        }

        return val;
         </#if>
     }
     </#if>

    <#if pt.valueType.isInteger>
    /**
     * Returns the argument incremented by one, throwing an exception if the result overflows.
     *
     * @param x the value to increment.
     * @return the result of increment by one.  If the value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public ${pt.primitive} incrementExact(${pt.primitive} x) {
        if (isNull(x)) {
            return ${pt.null};
        }

        <#if pt.primitive == "byte" || pt.primitive == "short">
        int val = Math.incrementExact(x);

        if ( val > ${pt.maxValue} || val < ${pt.minValue} || isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x);
        }

        return (${pt.primitive}) val;
        <#else>
        if (x == ${pt.maxValue}) {
            throw new ArithmeticException("Overflow: " + x);
        }

        ${pt.primitive} val = Math.incrementExact(x);

        if ( isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x);
        }

        return val;
        </#if>
    }
    </#if>

    <#if pt.valueType.isInteger>
    /**
     * Returns the argument decremented by one, throwing an exception if the result overflows.
     *
     * @param x the value to decrement.
     * @return the result of decrementing by one.  If the value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public ${pt.primitive} decrementExact(${pt.primitive} x) {
        if (isNull(x)) {
            return ${pt.null};
        }

        <#if pt.primitive == "byte" || pt.primitive == "short">
        int val = Math.decrementExact(x);

        if ( val > ${pt.maxValue} || val < ${pt.minValue} || isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x);
        }

        return (${pt.primitive}) val;
        <#else>
        if (x == ${pt.minValue}) {
            throw new ArithmeticException("Overflow: " + x);
        }

        ${pt.primitive} val = Math.decrementExact(x);

        if ( isNull(val) ) {
            throw new ArithmeticException("Overflow: " + x);
        }

        return val;
        </#if>
    }
    </#if>

     <#if pt.valueType.isInteger>
     /**
      * Returns the negation of the argument, throwing an exception if the result overflows.
      *
      * @param x the value to negate.
      * @return the negation of the argument.  If the value is null, returns null.
      * @throws ArithmeticException if the result overflows.
      */
     public static ${pt.primitive} negateExact(${pt.primitive} x) {
         if (isNull(x)) {
             return ${pt.null};
         }

         <#if pt.primitive == "byte" || pt.primitive == "short">
         int val = Math.negateExact(x);

         if ( val > ${pt.maxValue} || val < ${pt.minValue} || isNull(val) ) {
            throw new ArithmeticException("Overflow: -" + x);
         }

         return (${pt.primitive}) val;
         <#else>
         ${pt.primitive} val = Math.negateExact(x);

         if ( isNull(val) ) {
             throw new ArithmeticException("Overflow: -" + x);
         }

         return val;
         </#if>
     }
     </#if>

    <#if pt.valueType.isInteger>
     /**
      * Returns the largest (closest to positive infinity) int value that is less than or equal to the
      * algebraic quotient.
      *
      * @param x the dividend.
      * @param y the divisor.
      * @return the largest (closest to positive infinity) int value that is less than or equal to the
      *    algebraic quotient.  If either value is null, returns null.
      */
    static public ${pt.primitive} floorDiv(${pt.primitive} x, ${pt.primitive} y) {
        if (isNull(x) || isNull(y)) {
            return ${pt.null};
        }

        return (${pt.primitive}) Math.floorDiv(x, y);
    }
    </#if>

    <#if pt.valueType.isInteger>
     /**
      * Returns the floor modulus of the arguments.
      *
      * @param x the dividend.
      * @param y the divisor.
      * @return the floor modulus x.  If either value is null, returns null.
      */
    static public ${pt.primitive} floorMod(${pt.primitive} x, ${pt.primitive} y) {
        if (isNull(x) || isNull(y)) {
            return ${pt.null};
        }

        return (${pt.primitive}) Math.floorMod(x, y);
    }
    </#if>

    <#if pt.valueType.isFloat>
     /**
      * Returns the unbiased exponent used in the representation of the argument.
      *
      * @param x the value.
      * @return the unbiased exponent used in the representation of the argument.  If the value is null, returns null.
      */
    static public int getExponent(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_INT;
        }

        return Math.getExponent(x);
    }
    </#if>

    <#if pt.valueType.isFloat>
    /**
     * Returns the IEEE 754 remainder of the division of the arguments.
     *
     * @param x the dividend.
     * @param y the divisor.
     * @return the IEEE 754 remainder of the division of the arguments.  If either value is null, returns null.
     */
    static public ${pt.primitive} IEEEremainder(${pt.primitive} x, ${pt.primitive} y) {
        if (isNull(x) || isNull(y)) {
            return ${pt.null};
        }

        return (${pt.primitive}) Math.IEEEremainder(x, y);
    }
    </#if>

     <#if pt.valueType.isFloat>
     /**
      * Returns the floating-point number adjacent to the first argument in the direction of the second argument.
      *
      * @param start the starting value.
      * @param direction the direction.
      * @return the floating-point number adjacent to the first argument in the direction of the second argument.
      *    If either value is null, returns null.
      */
     static public ${pt.primitive} nextAfter(${pt.primitive} start, ${pt.primitive} direction) {
        if (isNull(start) || isNull(direction)) {
            return ${pt.null};
        }

        // skip over nulls
        ${pt.primitive} next = Math.nextAfter(start, direction);
        return isNull(next) ? Math.nextAfter(next, direction) : next;
     }
     </#if>

     <#if pt.valueType.isFloat>
     /**
      * Returns the floating-point number adjacent to the argument in the direction of positive infinity.
      *
      * @param x the value.
      * @return the floating-point number adjacent to the argument in the direction of positive infinity.
      *    If the value is null, returns null.
      */
     static public ${pt.primitive} nextUp(${pt.primitive} x) {
        if (isNull(x)) {
            return ${pt.null};
        }

        // skip over nulls
        ${pt.primitive} next = Math.nextUp(x);
        return isNull(next) ? Math.nextUp(next) : next;
     }
     </#if>

     <#if pt.valueType.isFloat>
     /**
      * Returns the floating-point number adjacent to the argument in the direction of negative infinity.
      *
      * @param x the value.
      * @return the floating-point number adjacent to the argument in the direction of negative infinity.
      *    If the value is null, returns null.
      */
     static public ${pt.primitive} nextDown(${pt.primitive} x) {
        if (isNull(x)) {
            return ${pt.null};
        }

        // skip over nulls
        ${pt.primitive} next = Math.nextDown(x);
        return isNull(next) ? Math.nextDown(next) : next;
     }
     </#if>

    /**
     * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
     *
     * @param x the angle in radians
     * @return the measurement of the angle x in degrees.  If the value is null, returns null.
     */
    static public double toDegrees(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.toDegrees(x);
    }

    /**
     * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
     *
     * @param x the angle in degrees
     * @return the measurement of the angle x in radians.  If the value is null, returns null.
     */
    static public double toRadians(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_DOUBLE;
        }

        return Math.toRadians(x);
    }

    <#if pt.valueType.isInteger>
    /**
     * Returns the value of the argument as an int, throwing an exception if the value overflows an int.
     *
     * @param x the value.
     * @return the value as an int.  If either value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public int toIntExact(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_INT;
        }

        <#if pt.primitive == "byte" || pt.primitive == "short" || pt.primitive == "int" >
        return x;
        <#else>
        final int val = Math.toIntExact(x);

        if ( isNull(val) ) {
            throw new ArithmeticException("Overflow: ${pt.primitive} value will not fit in an int " + x);
        }

        return val;
        </#if>
    }
    </#if>

    <#if pt.valueType.isInteger>
    /**
     * Returns the value of the argument as a short, throwing an exception if the value overflows a short.
     *
     * @param x the value.
     * @return the value as a short.  If either value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public short toShortExact(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_SHORT;
        }

        <#if pt.primitive == "byte" || pt.primitive == "short" >
        return x;
        <#else>
        final short val = (short) x;

        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE || isNull(val) ) {
            throw new ArithmeticException("Overflow: ${pt.primitive} value will not fit in a short " + x);
        }

        return val;
        </#if>
    }
    </#if>

    <#if pt.valueType.isInteger>
    /**
     * Returns the value of the argument as a byte, throwing an exception if the value overflows a byte.
     *
     * @param x the value.
     * @return the value as a byte.  If either value is null, returns null.
     * @throws ArithmeticException if the result overflows.
     */
    static public short toByteExact(${pt.primitive} x) {
        if (isNull(x)) {
            return NULL_BYTE;
        }

        <#if pt.primitive == "byte" >
        return x;
        <#else>
        final byte val = (byte) x;

        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE || isNull(val) ) {
            throw new ArithmeticException("Overflow: ${pt.primitive} value will not fit in a byte " + x);
        }

        return val;
        </#if>
    }
    </#if>

    <#if pt.valueType.isFloat>
    /**
     * Returns the size of an ulp of the argument. An ulp, unit in the last place, of a value is the positive
     * distance between this floating-point value and the value next larger in magnitude.
     * Note that for non-NaN x, ulp(-x) == ulp(x).
     *
     * @param x the value.
     * @return the size of an ulp of the argument.  If the value is null, returns null.
     */
    static public ${pt.primitive} ulp(${pt.primitive} x) {
        if (isNull(x)) {
            return ${pt.null};
        }

        return Math.ulp(x);
    }
    </#if>

    </#if>
    </#list>
}
