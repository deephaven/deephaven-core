/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function.comparators;

import java.util.Comparator;

import static io.deephaven.function.Basic.isNull;
import static io.deephaven.function.Numeric.isNaN;

/**
 * Comparator which is aware of NaN and Null values, in the Deephaven convention.
 *
 * @param <T> type to compare.
 */
public class NullNaNAwareComparator<T extends Comparable<? super T>> implements Comparator<T> {

    /**
     * Creates a comparator.
     */
    public NullNaNAwareComparator() {}

    private static <T> boolean isNanVal(T v) {
        if (v instanceof Float) {
            return Float.isNaN((Float) v);
        } else if (v instanceof Double) {
            return Double.isNaN((Double) v);
        } else {
            return false;
        }
    }

    @Override
    public int compare(final T o1, final T o2) {
        final boolean isNull1 = isNull(o1);
        final boolean isNull2 = isNull(o2);

        if (isNull1 && isNull2) {
            return 0;
        } else if (isNull1) {
            return -1;
        } else if (isNull2) {
            return 1;
        }

        final boolean isNaN1 = isNanVal(o1);
        final boolean isNaN2 = isNanVal(o2);

        // NaN is considered the greatest
        if (isNaN1 && isNaN2) {
            return 0;
        } else if (isNaN1) {
            return 1;
        } else if (isNaN2) {
            return -1;
        }

        return o1.compareTo(o2);
    }

}
