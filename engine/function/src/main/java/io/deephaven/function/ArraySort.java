//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import java.util.Arrays;
import java.util.Comparator;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Perform array sorts that respect Deephaven null values as the lowest possible values.
 */
public class ArraySort {
    public static void sort(char[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
        // Arrays.sort() places Deephaven NULL_CHAR (Character.MAX_VALUE) at the end of the slice. This code moves
        // them to the beginning of the slice.
        // [ data_1, ..., data_m, NULL_CHAR * n ]
        // ->
        // [ NULL_CHAR * n, data_1, ..., data_m ]
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndexInclusive] == NULL_CHAR) {
            // all are NULL_CHAR, already sorted
            return;
        }
        if (data[toIndexExclusive - 1] != NULL_CHAR) {
            // none are NULL_CHAR, already sorted
            return;
        }

        // We can start with fromIndexInclusive + 1 because we know that data[fromIndexInclusive] is not NULL_CHAR
        final int firstNullIdx = lowerBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_CHAR);
        final int nullCount = toIndexExclusive - firstNullIdx;
        System.arraycopy(data, fromIndexInclusive, data, fromIndexInclusive + nullCount,
                toIndexExclusive - fromIndexInclusive - nullCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + nullCount, NULL_CHAR);
    }

    public static void sort(byte[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(short[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(int[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(long[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(float[] data, int fromIndexInclusive, int toIndexExclusive) {
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
        // We need to fix up null (-Float.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // slice
        // [ -Inf * m, NULL_FLOAT * n, ...]
        // ->
        // [ NULL_FLOAT * n, -Inf * m, ...]
        if (data[fromIndexInclusive] != Float.NEGATIVE_INFINITY) {
            // Only chance that NULL_FLOAT is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        // We can start with fromIndexInclusive + 1 because we know that data[fromIndexInclusive] is NEGATIVE_INFINITY
        final int beginNullIdx = lowerBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_FLOAT);
        if (beginNullIdx >= toIndexExclusive || data[beginNullIdx] != NULL_FLOAT) {
            // No NULL_FLOAT
            return;
        }
        final int endNullIdx = upperBound(data, beginNullIdx + 1, toIndexExclusive, NULL_FLOAT);

        final int negInfCount = beginNullIdx - fromIndexInclusive;
        final int nullCount = endNullIdx - beginNullIdx;

        // Only some of the -Inf and some of the NULL_FLOATS are wrong. For example (with m = 3 and n = 5)
        // [ -Inf, -Inf, -Inf, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, ...]
        // ->
        // [ NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, unchanged, unchanged, -Inf, -Inf, -Inf, ...]

        final int numWrong = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + numWrong, NULL_FLOAT);
        Arrays.fill(data, endNullIdx - numWrong, endNullIdx, Float.NEGATIVE_INFINITY);
    }

    public static void sort(double[] data, int fromIndexInclusive, int toIndexExclusive) {
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
        // We need to fix up null (-Double.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // slice
        // [ -Inf * m, NULL_DOUBLE * n, ...]
        // ->
        // [ NULL_DOUBLE * n, -Inf * m, ...]
        if (data[fromIndexInclusive] != Double.NEGATIVE_INFINITY) {
            // Only chance that NULL_DOUBLE is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        // We can start with fromIndexInclusive + 1 because we know that data[fromIndexInclusive] is NEGATIVE_INFINITY
        final int beginNullIdx = lowerBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_DOUBLE);
        if (beginNullIdx >= toIndexExclusive || data[beginNullIdx] != NULL_DOUBLE) {
            // No NULL_DOUBLE
            return;
        }
        final int endNullIdx = upperBound(data, beginNullIdx + 1, toIndexExclusive, NULL_DOUBLE);

        final int negInfCount = beginNullIdx - fromIndexInclusive;
        final int nullCount = endNullIdx - beginNullIdx;

        // Only some of the -Inf and some of the NULL_DOUBLE are wrong. For example (with m = 3 and n = 5)
        // [ -Inf, -Inf, -Inf, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, ...]
        // ->
        // [ NULL_DOUBLE, NULL_DOUBLE, NULL_DOUBLE, unchanged, unchanged, -Inf, -Inf, -Inf, ...]

        final int numWrong = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + numWrong, NULL_DOUBLE);
        Arrays.fill(data, endNullIdx - numWrong, endNullIdx, Double.NEGATIVE_INFINITY);
    }

    private static final Comparator<Comparable<Object>> COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

    public static <T> void sort(T[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive, (Comparator<? super T>) COMPARATOR);
    }

    /**
     * @return the smallest {@code i} in {@code [begin, end)} where {@code a[i] >= key} (or {@code end} if no value
     *         matches). Uses {@code <} for comparison because this assumes {@code a} was sorted by
     *         {@link Arrays#sort(char[])}.
     */
    private static int lowerBound(char[] a, int begin, int end, char key) {
        while (begin < end) {
            final int mid = (begin + end) >>> 1;
            if (a[mid] < key) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }

    /**
     * @return the smallest {@code i} in {@code [begin, end)} where {@code a[i] >= key} (or {@code end} if no value
     *         matches). Uses {@link Float#compare(float, float)} because this assumes {@code a} was sorted by
     *         {@link Arrays#sort(float[])}.
     */
    private static int lowerBound(float[] a, int begin, int end, float key) {
        while (begin < end) {
            final int mid = (begin + end) >>> 1;
            if (Float.compare(a[mid], key) < 0) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }

    /**
     * @return the smallest {@code i} in {@code [begin, end)} where {@code a[i] > key}. Uses
     *         {@link Float#compare(float, float)} because this assumes {@code a} was sorted by
     *         {@link Arrays#sort(float[])}.
     */
    private static int upperBound(float[] a, int begin, int end, float key) {
        while (begin < end) {
            final int mid = (begin + end) >>> 1;
            if (Float.compare(a[mid], key) <= 0) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }

    /**
     * @return the smallest {@code i} in {@code [begin, end)} where {@code a[i] >= key} (or {@code end} if no value
     *         matches). Uses {@link Double#compare(double, double)} because this assumes {@code a} was sorted by
     *         {@link Arrays#sort(double[])}.
     */
    private static int lowerBound(double[] a, int begin, int end, double key) {
        while (begin < end) {
            final int mid = (begin + end) >>> 1;
            if (Double.compare(a[mid], key) < 0) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }

    /**
     * @return the smallest {@code i} in {@code [begin, end)} where {@code a[i] > key}. Uses
     *         {@link Double#compare(double, double)} because this assumes {@code a} was sorted by
     *         {@link Arrays#sort(double[])}.
     */
    private static int upperBound(double[] a, int begin, int end, double key) {
        while (begin < end) {
            final int mid = (begin + end) >>> 1;
            if (Double.compare(a[mid], key) <= 0) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin;
    }
}
