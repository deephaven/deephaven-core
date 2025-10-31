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

    private static final int NULL_FLOAT_BITS = Float.floatToIntBits(NULL_FLOAT);
    private static final int MIN_FINITE_FLOAT_BITS = Float.floatToIntBits(QueryConstants.MIN_FINITE_FLOAT);

    private static final long NULL_DOUBLE_BITS = Double.doubleToLongBits(NULL_DOUBLE);
    private static final long MIN_FINITE_DOUBLE_BITS = Double.doubleToLongBits(QueryConstants.MIN_FINITE_DOUBLE);

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

    public static void sort(char[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
        // We need to "fix-up" the nulls (Character.MAX_VALUE) from end of subset to beginning of subset.
        // [ (...,) data_1, ..., data_m, NULL_CHAR_1, ..., NULL_CHAR_n (,...) ]
        // ->
        // [ (...,) NULL_CHAR_1, ..., NULL_CHAR_n, data_1, ..., data_m (,...) ]
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
        // We know first position is not NULL_CHAR; we don't want to exclude the last position, b/c it may be the
        // only NULL_CHAR.
        final int firstNullIx = binarySearchFirst(data, fromIndexInclusive + 1, toIndexExclusive, NULL_CHAR);
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
        // We need to "fix-up" null (-Float.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // subset
        // [ (...,) -Inf_1, ..., -Inf_m, NULL_FLOAT_1, ..., NULL_FLOAT_m, ..., (,...)]
        // ->
        // [ (...,) NULL_FLOAT_1, ..., NULL_FLOAT_m, -Inf_1, ..., -Inf_n, ..., (,...)]
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndexInclusive] != Float.NEGATIVE_INFINITY) {
            // Only chance that NULL_FLOAT is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        final int firstNullIx =
                binarySearchFirst(data, fromIndexInclusive + 1, toIndexExclusive, NULL_FLOAT, NULL_FLOAT_BITS);
        if (firstNullIx < 0) {
            // No NULL_FLOAT
            return;
        }
        final int negInfCount = firstNullIx - fromIndexInclusive;
        final int nullCount;
        {
            final int firstFloatAfterNull =
                    binarySearchFirst(data, firstNullIx + 1, toIndexExclusive, MIN_FINITE_FLOAT, MIN_FINITE_FLOAT_BITS);
            final int lastNullIx = (firstFloatAfterNull >= 0 ? firstFloatAfterNull : ~firstFloatAfterNull) - 1;
            nullCount = lastNullIx - firstNullIx + 1;
        }
        final int size = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + size, NULL_FLOAT);
        {
            final int negInftoIndexExclusive = fromIndexInclusive + nullCount + negInfCount;
            Arrays.fill(data, negInftoIndexExclusive - size, negInftoIndexExclusive, Float.NEGATIVE_INFINITY);
        }
    }

    public static void sortUnsafe(double[] data, int fromIndexInclusive, int toIndexExclusive) {
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive);
    }

    public static void sort(double[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
        // We need to "fix-up" null (-Double.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // subset
        // [ (...,) -Inf_1, ..., -Inf_m, NULL_DOUBLE_1, ..., NULL_DOUBLE_m, ..., (,...)]
        // ->
        // [ (...,) NULL_DOUBLE_1, ..., NULL_DOUBLE_m, -Inf_1, ..., -Inf_n, ..., (,...)]
        if (toIndexExclusive - fromIndexInclusive <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndexInclusive] != Double.NEGATIVE_INFINITY) {
            // Only chance that NULL_DOUBLE is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        final int firstNullIx =
                binarySearchFirst(data, fromIndexInclusive + 1, toIndexExclusive, NULL_DOUBLE, NULL_DOUBLE_BITS);
        if (firstNullIx < 0) {
            // No NULL_DOUBLE
            return;
        }
        final int negInfCount = firstNullIx - fromIndexInclusive;
        final int nullCount;
        {
            final int firstDoubleAfterNull = binarySearchFirst(data, firstNullIx + 1, toIndexExclusive,
                    MIN_FINITE_DOUBLE, MIN_FINITE_DOUBLE_BITS);
            final int lastNullIx = (firstDoubleAfterNull >= 0 ? firstDoubleAfterNull : ~firstDoubleAfterNull) - 1;
            nullCount = lastNullIx - firstNullIx + 1;
        }
        final int size = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndexInclusive, fromIndexInclusive + size, NULL_DOUBLE);
        {
            final int negInftoIndexExclusive = fromIndexInclusive + nullCount + negInfCount;
            Arrays.fill(data, negInftoIndexExclusive - size, negInftoIndexExclusive, Double.NEGATIVE_INFINITY);
        }
    }

    private static final Comparator<Comparable<Object>> COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

    public static <T> void sort(T[] data, int fromIndexInclusive, int toIndexExclusive) {
        sortUnsafe(data, fromIndexInclusive, toIndexExclusive);
    }

    public static <T> void sortUnsafe(T[] data, int fromIndexInclusive, int toIndexExclusive) {
        // noinspection unchecked
        Arrays.sort(data, fromIndexInclusive, toIndexExclusive, (Comparator<? super T>) COMPARATOR);
    }

    private static int binarySearchFirst(char[] a, int fromIndexInclusive, int toIndexExclusive, char key) {
        int low = fromIndexInclusive;
        int high = toIndexExclusive - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final char midVal = a[mid];
            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else if (low != mid) {
                // This is where we differ from Arrays.binarySearch;
                // we continue searching downwards to find the first instance.
                high = mid;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private static int binarySearchFirst(float[] a, int fromIndexInclusive, int toIndexExclusive, float key,
            int keyBits) {
        int low = fromIndexInclusive;
        int high = toIndexExclusive - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final float midVal = a[mid];
            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                final int midBits = Float.floatToIntBits(midVal);
                if (midBits < keyBits) {
                    // (-0.0, 0.0) or (!NaN, NaN)
                    low = mid + 1;
                } else if (midBits > keyBits) {
                    // (0.0, -0.0) or (NaN, !NaN)
                    high = mid - 1;
                } else if (low != mid) {
                    // This is where we differ from Arrays.binarySearch;
                    // we continue searching downwards to find the first instance.
                    high = mid;
                } else {
                    return mid;
                }
            }
        }
        return -(low + 1);
    }

    private static int binarySearchFirst(double[] a, int fromIndexInclusive, int toIndexExclusive, double key,
            long keyBits) {
        int low = fromIndexInclusive;
        int high = toIndexExclusive - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final double midVal = a[mid];
            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                final long midBits = Double.doubleToLongBits(midVal);
                if (midBits < keyBits) {
                    // (-0.0, 0.0) or (!NaN, NaN)
                    low = mid + 1;
                } else if (midBits > keyBits) {
                    // (0.0, -0.0) or (NaN, !NaN)
                    high = mid - 1;
                } else if (low != mid) {
                    // This is where we differ from Arrays.binarySearch;
                    // we continue searching downwards to find the first instance.
                    high = mid;
                } else {
                    return mid;
                }
            }
        }
        return -(low + 1);
    }
}
