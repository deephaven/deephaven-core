
package io.deephaven.function;

import io.deephaven.base.verify.Require;
import io.deephaven.vector.*;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.util.Arrays;

import static io.deephaven.base.CompareUtils.compare;
import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Basic.*;
import static io.deephaven.function.Cast.castDouble;

/**
 * A set of commonly used numeric functions that can be applied to numeric types.
 */
@SuppressWarnings({"RedundantCast", "unused", "ManualMinMaxCalculation"})
public class Numeric {


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
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            T c = values.get(i);
            if (!isNull(c) && ( val == null || c.compareTo(val) > 0)) {
                val = c;
                index = i;
                count++;
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
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            T c = values.get(i);
            if (!isNull(c) && ( val == null || c.compareTo(val) < 0)) {
                val = c;
                index = i;
                count++;
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

        return countPos(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Counts the number of positive values.
     *
     * @param values values.
     * @return number of positive values.
     */
    public static long countPos(${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c) && c > 0) {
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

        return countNeg(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Counts the number of negative values.
     *
     * @param values values.
     * @return number of negative values.
     */
    public static long countNeg(${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c) && c < 0) {
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

        return countZero(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Counts the number of zero values.
     *
     * @param values values.
     * @return number of zero values.
     */
    public static long countZero(${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        long count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c) && c == 0) {
                count++;
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

        return avg(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the mean.  Null values are excluded.
     *
     * @param values values.
     * @return mean of non-null values.
     */
    public static double avg(${pt.dbArray} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c)) {
                sum += c;
                count++;
            }
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

        return absAvg(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the mean of the absolute values of values.  Null values are excluded.
     *
     * @param values values.
     * @return mean of the absolute value of non-null values.
     */
    public static double absAvg(${pt.dbArray} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c)) {
                sum += Math.abs(c);
                count++;
            }
        }

        return sum / count;
    }

    /**
     * Returns the variance.  Null values are excluded.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(${pt.boxed}[] values) {
        return var(unbox(values));
    }

    /**
     * Returns the variance.  Null values are excluded.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return var(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the variance.  Null values are excluded.
     *
     * @param values values.
     * @return variance of non-null values.
     */
    public static double var(${pt.dbArray} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c)) {
                sum += (double)c;
                sum2 += (double)c * (double)c;
                count++;
            }
        }

        return sum2 / (count - 1) - sum * sum / count / (count - 1);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted variance.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new ${pt.dbArrayDirect}(values), new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(${pt.primitive}[] values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(new ${pt.dbArrayDirect}(values), weights);
    }

    /**
     * Returns the weighted variance.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(${pt.dbArray} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wvar(values, new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted variance.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted variance of non-null values.
     */
    public static double wvar(${pt.dbArray} values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final long n = values.size();

        if(n != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        double sum = 0;
        double sum2 = 0;
        double count = 0;

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            ${pt2.primitive} w = weights.get(i);
            if (!isNull(c) && !isNull(w)) {
                sum += w * c;
                sum2 += w * c * c;
                count += w;
            }
        }

        return sum2 / count - sum * sum / count / count;
    }

    </#if>
    </#list>


    /**
     * Returns the standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(${pt.boxed}[] values) {
        return std(unbox(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return std(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @return standard deviation of non-null values.
     */
    public static double std(${pt.dbArray} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        final double v = var(values);
        return v == NULL_DOUBLE ? NULL_DOUBLE : Math.sqrt(v);
    }

    <#list primitiveTypes as pt2>
    <#if pt2.valueType.isNumber >

    /**
     * Returns the weighted standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new ${pt.dbArrayDirect}(values), new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(${pt.primitive}[] values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(new ${pt.dbArrayDirect}(values), weights);
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(${pt.dbArray} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wstd(values, new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted standard deviation.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard deviation of non-null values.
     */
    public static double wstd(${pt.dbArray} values, ${pt2.dbArray} weights) {
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

        return ste(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the standard error.  Null values are excluded.
     *
     * @param values values.
     * @return standard error of non-null values.
     */
    public static double ste(${pt.dbArray} values) {
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

        return wste(new ${pt.dbArrayDirect}(values), new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.primitive}[] values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(new ${pt.dbArrayDirect}(values), weights);
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.dbArray} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wste(values, new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted standard error.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted standard error of non-null values.
     */
    public static double wste(${pt.dbArray} values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        if(values.size() != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        // see https://stats.stackexchange.com/questions/25895/computing-standard-error-in-weighted-mean-estimation
        double sumw = 0;
        double sumw2 = 0;
        final long n = values.size();

        for(long i=0; i<n; i++) {
            final ${pt.primitive} v = values.get(i);
            final ${pt2.primitive} w = weights.get(i);

            if(!isNull(v) && !isNull(w)){
                sumw += w;
                sumw2 += w*w;
            }
        }

        final double s = wstd(values, weights);
        return s == NULL_DOUBLE ? NULL_DOUBLE : s * Math.sqrt(sumw2/sumw/sumw);
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

        return tstat(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @return t-statistic of non-null values.
     */
    public static double tstat(${pt.dbArray} values) {
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

        return wtstat(new ${pt.dbArrayDirect}(values), new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.primitive}[] values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(new ${pt.dbArrayDirect}(values), weights);
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.dbArray} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wtstat(values, new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted t-statistic.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted t-statistic of non-null values.
     */
    public static double wtstat(${pt.dbArray} values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final double a = wavg(values, weights);
        final double s = wste(values, weights);
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
    public static ${pt.primitive} max(${pt.dbArray} values) {
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

        return max(new ${pt.dbArrayDirect}(values));
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
    public static ${pt.primitive} min(${pt.dbArray} values) {
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

        return min(new ${pt.dbArrayDirect}(values));
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

        return indexOfMax(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the index of the maximum value.
     *
     * @param values values.
     * @return index of the maximum value.
     */
    public static long indexOfMax(${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        ${pt.primitive} val = ${pt.minValue};
        long index = NULL_LONG;
        long count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c) && (c > val || (c == val && count == 0))) {
                val = c;
                index = i;
                count++;
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

        return indexOfMin(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the index of the minimum value.
     *
     * @param values values.
     * @return index of the minimum value.
     */
    public static long indexOfMin(${pt.dbArray} values) {
        if (values == null) {
            return NULL_LONG;
        }

        ${pt.primitive} val = ${pt.maxValue};
        long index = NULL_LONG;
        long count = 0;
        final long n = values.size();

        for (int i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c) && (c < val || (c == val && count == 0) )) {
                val = c;
                index = i;
                count++;
            }
        }

        return count == 0 ? NULL_LONG : index;
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(${pt.boxed}[] values) {
        return median(unbox(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(${pt.primitive}... values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        return median(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the median.
     *
     * @param values values.
     * @return median.
     */
    public static double median(${pt.dbArray} values) {
        if (values == null) {
            return NULL_DOUBLE;
        }

        int n = values.intSize("median");

        if (n == 0) {
            return Double.NaN;
        } else {
            ${pt.primitive}[] copy = values.toArray();
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
    public static double percentile(double percentile, ${pt.primitive}... values) {
        if(values == null){
            return NULL_DOUBLE;
        }

        return percentile(percentile, new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the percentile.
     *
     * @param percentile percentile to compute.
     * @param values values.
     * @return percentile.
     */
    public static double percentile(double percentile, ${pt.dbArray} values) {
        if(values == null){
            return NULL_DOUBLE;
        }

        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Invalid percentile = " + percentile);
        }

        int n = values.intSize("percentile");
        ${pt.primitive}[] copy = values.toArray();
        Arrays.sort(copy);

        int idx = (int) Math.round(percentile * (n - 1));
        return copy[idx];
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

        return cov(new ${pt.dbArrayDirect}(values0), new ${pt2.dbArray}Direct(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.primitive}[] values0, ${pt2.dbArray} values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(new ${pt.dbArrayDirect}(values0), values1);
    }

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.dbArray} values0, ${pt2.primitive}[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cov(values0, new ${pt2.dbArray}Direct(values1));
    }

    /**
     * Returns the covariance.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return covariance of non-null values.
     */
    public static double cov(${pt.dbArray} values0, ${pt2.dbArray} values1) {
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
        final long n = values0.size();

        for (long i = 0; i < n; i++) {
            if (!isNull(values0.get(i)) && !isNull(values1.get(i))) {
                sum0 += values0.get(i);
                sum1 += values1.get(i);
                sum01 += values0.get(i) * values1.get(i);
                count++;
            }
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

        return cor(new ${pt.dbArrayDirect}(values0), new ${pt2.dbArray}Direct(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.primitive}[] values0, ${pt2.dbArray} values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(new ${pt.dbArrayDirect}(values0), values1);
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.dbArray} values0, ${pt2.primitive}[] values1) {
        if (values0 == null || values1 == null) {
            return NULL_DOUBLE;
        }

        return cor(values0, new ${pt2.dbArray}Direct(values1));
    }

    /**
     * Returns the correlation.  Null values are excluded.
     *
     * @param values0 1st set of values.
     * @param values1 2nd set of values.
     * @return correlation of non-null values.
     */
    public static double cor(${pt.dbArray} values0, ${pt2.dbArray} values1) {
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
        final long n = values0.size();

        for (long i = 0; i < n; i++) {
            if (!isNull(values0.get(i)) && !isNull(values1.get(i))) {
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

    </#if>
    </#list>


    /**
     * Returns the sum.  Null values are excluded.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static ${pt.primitive} sum(${pt.dbArray} values) {
        if (values == null) {
            return ${pt.null};
        }

        double sum = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c)) {
                sum += c;
            }
        }
        return (${pt.primitive}) (sum);
    }

    /**
     * Returns the sum.  Null values are excluded.
     *
     * @param values values.
     * @return sum of non-null values.
     */
    public static ${pt.primitive} sum(${pt.primitive}... values) {
        if (values == null) {
            return ${pt.null};
        }

        return sum(new ${pt.dbArrayDirect}(values));
    }

    /**
     * Returns the product.  Null values are excluded.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static ${pt.primitive} product(${pt.dbArray} values) {
        if (values == null) {
            return ${pt.null};
        }

        ${pt.primitive} prod = 1;
        int count = 0;
        final long n = values.size();

        for (long i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            if (!isNull(c)) {
                count++;
                prod *= c;
            }
        }

        if(count == 0){
            return ${pt.null};
        }

        return (${pt.primitive}) (prod);
    }

    /**
     * Returns the product.  Null values are excluded.
     *
     * @param values values.
     * @return product of non-null values.
     */
    public static ${pt.primitive} product(${pt.primitive}... values) {
        if (values == null) {
            return ${pt.null};
        }

        return product(new ${pt.dbArrayDirect}(values));
    }


    /**
     * Returns the cumulative minimum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative min of non-null values.
     */
    public static ${pt.primitive}[] cumMin(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[0];
        }

        ${pt.primitive}[] result = new ${pt.primitive}[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (${pt.primitive})Math.min(result[i - 1],  values[i]);
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
    public static ${pt.primitive}[] cumMax(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[0];
        }

        ${pt.primitive}[] result = new ${pt.primitive}[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            result[i] = compare(result[i-1], values[i]) > 0 ? result[i-1] : values[i];
        }

        return result;
    }


   /**
     * Returns the cumulative sum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static ${pt.primitive}[] cumsum(${pt.boxed}[] values) {
        return cumsum(unbox(values));
    }

    /**
     * Returns the cumulative sum.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative sum of non-null values.
     */
    public static ${pt.primitive}[] cumsum(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[0];
        }

        ${pt.primitive}[] result = new ${pt.primitive}[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (${pt.primitive}) (result[i - 1] + values[i]);
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
    public static ${pt.primitive}[] cumsum(${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new ${pt.primitive}[0];
        }

        final int n = values.intSize("cumsum");
        ${pt.primitive}[] result = new ${pt.primitive}[n];
        result[0] = values.get(0);

        for (int i = 1; i < n; i++) {
            if (isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (${pt.primitive}) (result[i - 1] + values.get(i));
            }
        }

        return result;
    }

    /**
     * Returns the cumulative product.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static ${pt.primitive}[] cumprod(${pt.boxed}[] values) {
        return cumprod(unbox(values));
    }

    /**
     * Returns the cumulative product.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static ${pt.primitive}[] cumprod(${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[0];
        }

        ${pt.primitive}[] result = new ${pt.primitive}[values.length];
        result[0] = values[0];

        for (int i = 1; i < values.length; i++) {
            if (isNull(result[i - 1])) {
                result[i] = values[i];
            } else if (isNull(values[i])) {
                result[i] = result[i - 1];
            } else {
                result[i] = (${pt.primitive}) (result[i - 1] * values[i]);
            }
        }

        return result;
    }

    /**
     * Returns the cumulative product.  Null values are excluded.
     *
     * @param values values.
     * @return cumulative product of non-null values.
     */
    public static ${pt.primitive}[] cumprod(${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new ${pt.primitive}[0];
        }

        final int n = values.intSize("cumprod");
        ${pt.primitive}[] result = new ${pt.primitive}[n];
        result[0] = values.get(0);

        for (int i = 1; i < n; i++) {
            if (isNull(result[i - 1])) {
                result[i] = values.get(i);
            } else if (isNull(values.get(i))) {
                result[i] = result[i - 1];
            } else {
                result[i] = (${pt.primitive}) (result[i - 1] * values.get(i));
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
    public static double wsum(${pt.primitive}[] values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new ${pt.dbArrayDirect}(values), new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(${pt.primitive}[] values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(new ${pt.dbArrayDirect}(values), weights);
    }

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(${pt.dbArray} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wsum(values, new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted sum.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted sum of non-null values.
     */
    public static double wsum(${pt.dbArray} values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final long n = values.size();

        if(n != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        double vsum = 0;

        for (int i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            ${pt2.primitive} w = weights.get(i);
            if (!isNull(c) && !isNull(w)) {
                vsum += c * w;
            }
        }

        return vsum;
    }

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

        return wavg(new ${pt.dbArrayDirect}(values), new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.primitive}[] values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(new ${pt.dbArrayDirect}(values), weights);
    }

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.dbArray} values, ${pt2.primitive}[] weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        return wavg(values, new ${pt2.dbArray}Direct(weights));
    }

    /**
     * Returns the weighted average.  Null values are excluded.
     *
     * @param values values.
     * @param weights weights
     * @return weighted average of non-null values.
     */
    public static double wavg(${pt.dbArray} values, ${pt2.dbArray} weights) {
        if (values == null || weights == null) {
            return NULL_DOUBLE;
        }

        final long n = values.size();

        if(n != weights.size()){
            throw new IllegalArgumentException("Incompatible input sizes: " + values.size() + ", " + weights.size());
        }

        double vsum = 0;
        double wsum = 0;

        for (int i = 0; i < n; i++) {
            ${pt.primitive} c = values.get(i);
            ${pt2.primitive} w = weights.get(i);
            if (!isNull(c) && !isNull(w)) {
                vsum += c * w;
                wsum += w;
            }
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
        if(step == 0) {
            return new ${pt.primitive}[0];
        }

        final int n = (int)((end-start)/step);

        if(n < 0) {
            return new ${pt.primitive}[0];
        }

        final ${pt.primitive}[] result = new ${pt.primitive}[n+1];

        for(int i=0; i<=n; i++){
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
        return replaceIfNaN(new ${pt.dbArrayDirect}(values), replacement);
    }

    /**
     * Replaces values that are NaN with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is NaN.
     * @return array containing value, if value is not NaN, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNaN(${pt.dbArray} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNaN");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        for (int i = 0; i < n; i++) {
            result[i] = replaceIfNaN(values.get(i), replacement);
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
        return replaceIfNullNaN(new ${pt.dbArrayDirect}(values), replacement);
    }

    /**
     * Replaces values that are NaN or null according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is NaN or null according to Deephaven convention.
     * @return array containing value, if value is neither NaN nor null according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNullNaN(${pt.dbArray} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNullNaN");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        for (int i = 0; i < n; i++) {
            result[i] = replaceIfNullNaN(values.get(i), replacement);
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
        return replaceIfNonFinite(new ${pt.dbArrayDirect}(values), replacement);
    }

    /**
     * Replaces values that are not finite according to Deephaven convention with a specified value.
     *
     * @param values the values.
     * @param replacement replacement to use when value is not finite according to Deephaven convention.
     * @return array containing value, if value is finite according to Deephaven convention, replacement otherwise.
     */
    static public ${pt.primitive}[] replaceIfNonFinite(${pt.dbArray} values, ${pt.primitive} replacement) {
        final int n = values.intSize("replaceIfNonFinite");
        ${pt.primitive}[] result = new ${pt.primitive}[n];

        for (int i = 0; i < n; i++) {
            result[i] = replaceIfNonFinite(values.get(i), replacement);
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

    </#if>
    </#list>
}
