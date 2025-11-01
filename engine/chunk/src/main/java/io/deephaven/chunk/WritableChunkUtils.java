//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import io.deephaven.util.QueryConstants;

import java.util.Arrays;
import java.util.Comparator;

import static io.deephaven.util.QueryConstants.MIN_FINITE_DOUBLE;
import static io.deephaven.util.QueryConstants.MIN_FINITE_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

final class WritableChunkUtils {
    @SuppressWarnings("unused")
    public static void sort(boolean[] data, int fromIndexInclusive, int toIndexExclusive) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unused")
    public static void sortUnsafe(boolean[] data, int fromIndexInclusive, int toIndexExclusive) {
        throw new UnsupportedOperationException();
    }

    public static void sortUnsafe(char[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    // These assertions reflect sorting assumptions made by this code. If these assertions
    // change, this code will need to be carefully audited.
    static {
        assert(NULL_CHAR == Character.MAX_VALUE);
        assert(NULL_FLOAT == -Float.MAX_VALUE);
        assert(Float.NEGATIVE_INFINITY < NULL_FLOAT);
    }

    public static void sort(char[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);

        // sortUnsafe() places Deephaven NULL_CHAR (Character.MAX_VALUE) at the end of the slice. This code moves them to the
        // beginning of the slice.
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
        final int firstNullIx = lowerBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_CHAR);
        final int nullCount = toIndexExclusive - firstNullIx;
        System.arraycopy(data, fromIndexInclusive, data, fromIndexInclusive + nullCount,
                toIndexExclusive - fromIndexInclusive - nullCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + nullCount, NULL_CHAR);
    }

    public static void sortUnsafe(byte[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(byte[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sortUnsafe(short[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(short[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sortUnsafe(int[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(int[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sortUnsafe(long[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(long[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sortUnsafe(float[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(float[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
        // We need to fix up null (-Float.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // slice
        // [ -Inf * m, NULL_FLOAT * n, ...]
        // ->
        // [ NULL_FLOAT * n, -Inf * m, ...]
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndexInclusive] != Float.NEGATIVE_INFINITY) {
            // Only chance that NULL_FLOAT is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        // We can start with fromIndexInclusive + 1 because we know that data[fromIndexInclusive] is not NULL_FLOAT
        final int beginNullIx = lowerBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_FLOAT);
        if (beginNullIx == toIndexExclusive || data[beginNullIx] != NULL_FLOAT) {
            // No NULL_FLOAT
            return;
        }
        final int endNullIx = upperBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_FLOAT);

        final int negInfCount = beginNullIx - fromIndexInclusive;
        final int nullCount = endNullIx - beginNullIx;

        // Only some of the -Inf and some of the NULL_FLOATS are wrong. For example (with m = 3 and n = 5)
        // [ -Inf, -Inf, -Inf, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, ...]
        // ->
        // [ NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, unchanged, unchanged, -Inf, -Inf, -Inf, ...]

        final int numWrong = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + numWrong, NULL_FLOAT);
        Arrays.fill(data, endNullIx - numWrong, endNullIx, Float.NEGATIVE_INFINITY);
    }

    public static void sortUnsafe(double[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(double[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
        // We need to fix up null (-Double.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // slice
        // [ -Inf * m, NULL_DOUBLE * n, ...]
        // ->
        // [ NULL_DOUBLE * n, -Inf * m, ...]
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndexInclusive] != Double.NEGATIVE_INFINITY) {
            // Only chance that NULL_DOUBLE is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        // We can start with fromIndexInclusive + 1 because we know that data[fromIndexInclusive] is not NULL_DOUBLE
        final int beginNullIx = lowerBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_DOUBLE);
        if (beginNullIx == toIndexExclusive || data[beginNullIx] != NULL_DOUBLE) {
            // No NULL_DOUBLE
            return;
        }
        final int endNullIx = upperBound(data, fromIndexInclusive + 1, toIndexExclusive, NULL_DOUBLE);

        final int negInfCount = beginNullIx - fromIndexInclusive;
        final int nullCount = endNullIx - beginNullIx;

        // Only some of the -Inf and some of the NULL_FLOATS are wrong. For example (with m = 3 and n = 5)
        // [ -Inf, -Inf, -Inf, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, ...]
        // ->
        // [ NULL_FLOAT, NULL_FLOAT, NULL_FLOAT, unchanged, unchanged, -Inf, -Inf, -Inf, ...]

        final int numWrong = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + numWrong, NULL_DOUBLE);
        Arrays.fill(data, endNullIx - numWrong, endNullIx, Double.NEGATIVE_INFINITY);
    }

    private static final Comparator<Comparable<Object>> COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

    public static <T> void sort(T[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
    }

    public static <T> void sortUnsafe(T[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive, (Comparator<? super T>) COMPARATOR);
    }

    /**
     * @return the smallest i in [begin, end) where a[i] >= key, or end if no such element
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
     * @return the smallest i in [begin, end) where a[i] >= key, or end if no such element
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
     * @return the smallest i in [begin, end) where a[i] > key, or end if no such element
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
     * @return the smallest i in [begin, end) where a[i] >= key, or end if no such element
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
     * @return the smallest i in [begin, end) where a[i] > key, or end if no such element
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
