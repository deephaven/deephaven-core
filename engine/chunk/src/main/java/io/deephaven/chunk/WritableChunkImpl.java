//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk;

import java.util.Arrays;
import java.util.Comparator;

import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

final class WritableChunkImpl {

    private static final float FLOAT_AFTER_NULL = Math.nextUp(NULL_FLOAT);
    private static final double DOUBLE_AFTER_NULL = Math.nextUp(NULL_DOUBLE);

    private static final int NULL_FLOAT_BITS = Float.floatToIntBits(NULL_FLOAT);
    private static final int FLOAT_AFTER_NULL_BITS = Float.floatToIntBits(FLOAT_AFTER_NULL);

    private static final long NULL_DOUBLE_BITS = Double.doubleToLongBits(NULL_DOUBLE);
    private static final long DOUBLE_AFTER_NULL_BITS = Double.doubleToLongBits(DOUBLE_AFTER_NULL);

    public static void sort(boolean[] data, int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    public static void sortUnsafe(boolean[] data, int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    public static void sortUnsafe(char[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(char[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
        // We need to "fix-up" the nulls (Character.MAX_VALUE) from end of subset to beginning of subset.
        // [ (...,) data_1, ..., data_m, NULL_CHAR_1, ..., NULL_CHAR_n (,...) ]
        // ->
        // [ (...,) NULL_CHAR_1, ..., NULL_CHAR_n, data_1, ..., data_m (,...) ]
        if (toIndex - fromIndex <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndex] == NULL_CHAR) {
            // all are NULL_CHAR, already sorted
            return;
        }
        if (data[toIndex - 1] != NULL_CHAR) {
            // none are NULL_CHAR, already sorted
            return;
        }
        // We know first position is not NULL_CHAR; we don't want to exclude the last position, b/c it may be the
        // only NULL_CHAR.
        final int firstNullIx = binarySearchFirst(data, fromIndex + 1, toIndex, NULL_CHAR);
        assert firstNullIx > 0;
        final int nullCount = toIndex - firstNullIx;
        System.arraycopy(data, fromIndex, data, fromIndex + nullCount, toIndex - fromIndex - nullCount);
        Arrays.fill(data, fromIndex, fromIndex + nullCount, NULL_CHAR);
    }

    public static void sortUnsafe(byte[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(byte[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
    }

    public static void sortUnsafe(short[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(short[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
    }

    public static void sortUnsafe(int[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(int[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
    }

    public static void sortUnsafe(long[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(long[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
    }

    public static void sortUnsafe(float[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(float[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
        // We need to "fix-up" null (-Float.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // subset
        // [ (...,) -Inf_1, ..., -Inf_m, NULL_FLOAT_1, ..., NULL_FLOAT_m, ..., (,...)]
        // ->
        // [ (...,) NULL_FLOAT_1, ..., NULL_FLOAT_m, -Inf_1, ..., -Inf_n, ..., (,...)]
        if (toIndex - fromIndex <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndex] != Float.NEGATIVE_INFINITY) {
            // Only chance that NULL_FLOAT is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        final int firstNullIx = binarySearchFirst(data, fromIndex + 1, toIndex, NULL_FLOAT, NULL_FLOAT_BITS);
        if (firstNullIx < 0) {
            // No NULL_FLOAT
            return;
        }
        final int negInfCount = firstNullIx - fromIndex;
        final int nullCount;
        {
            final int firstFloatAfterNull = binarySearchFirst(data, firstNullIx + 1, toIndex, FLOAT_AFTER_NULL,
                    FLOAT_AFTER_NULL_BITS);
            final int lastNullIx = (firstFloatAfterNull >= 0 ? firstFloatAfterNull : -firstFloatAfterNull - 1) - 1;
            nullCount = lastNullIx - firstNullIx + 1;
        }
        final int size = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndex, fromIndex + size, NULL_FLOAT);
        {
            final int negInfToIndex = fromIndex + nullCount + negInfCount;
            Arrays.fill(data, negInfToIndex - size, negInfToIndex, Float.NEGATIVE_INFINITY);
        }
    }

    public static void sortUnsafe(double[] data, int fromIndex, int toIndex) {
        Arrays.sort(data, fromIndex, toIndex);
    }

    public static void sort(double[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
        // We need to "fix-up" null (-Double.MAX_VALUE) vs -Infinity, which we know will be at the beginning of the
        // subset
        // [ (...,) -Inf_1, ..., -Inf_m, NULL_DOUBLE_1, ..., NULL_DOUBLE_m, ..., (,...)]
        // ->
        // [ (...,) NULL_DOUBLE_1, ..., NULL_DOUBLE_m, -Inf_1, ..., -Inf_n, ..., (,...)]
        if (toIndex - fromIndex <= 1) {
            // size 0 or 1, already sorted
            return;
        }
        if (data[fromIndex] != Double.NEGATIVE_INFINITY) {
            // Only chance that NULL_DOUBLE is mis-sorted is when -Infinity is present, and we know that only happens
            // when it's in the first position
            return;
        }
        final int firstNullIx = binarySearchFirst(data, fromIndex + 1, toIndex, NULL_DOUBLE, NULL_DOUBLE_BITS);
        if (firstNullIx < 0) {
            // No NULL_DOUBLE
            return;
        }
        final int negInfCount = firstNullIx - fromIndex;
        final int nullCount;
        {
            final int firstDoubleAfterNull = binarySearchFirst(data, firstNullIx + 1, toIndex, DOUBLE_AFTER_NULL,
                    DOUBLE_AFTER_NULL_BITS);
            final int lastNullIx = (firstDoubleAfterNull >= 0 ? firstDoubleAfterNull : -firstDoubleAfterNull - 1) - 1;
            nullCount = lastNullIx - firstNullIx + 1;
        }
        final int size = Math.min(nullCount, negInfCount);
        Arrays.fill(data, fromIndex, fromIndex + size, NULL_DOUBLE);
        {
            final int negInfToIndex = fromIndex + nullCount + negInfCount;
            Arrays.fill(data, negInfToIndex - size, negInfToIndex, Double.NEGATIVE_INFINITY);
        }
    }

    private static final Comparator<Comparable<Object>> COMPARATOR = Comparator.nullsFirst(Comparator.naturalOrder());

    public static <T> void sort(T[] data, int fromIndex, int toIndex) {
        sortUnsafe(data, fromIndex, toIndex);
    }

    public static <T> void sortUnsafe(T[] data, int fromIndex, int toIndex) {
        // noinspection unchecked
        Arrays.sort(data, fromIndex, toIndex, (Comparator<? super T>) COMPARATOR);
    }

    private static int binarySearchFirst(char[] a, int fromIndex, int toIndex, char key) {
        int low = fromIndex;
        int high = toIndex - 1;
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

    private static int binarySearchFirst(float[] a, int fromIndex, int toIndex, float key, int keyBits) {
        int low = fromIndex;
        int high = toIndex - 1;
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

    private static int binarySearchFirst(double[] a, int fromIndex, int toIndex, double key, long keyBits) {
        int low = fromIndex;
        int high = toIndex - 1;
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
