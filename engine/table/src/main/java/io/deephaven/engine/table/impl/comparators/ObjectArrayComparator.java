//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.comparators;

import io.deephaven.util.compare.ObjectComparisons;

import java.util.Comparator;

/**
 * Lexicographicaly compares two arrays using Deephaven ordering for the elements.
 *
 * <p>
 * Note, although this is equivalent to {@link CharArrayComparator}, it is not replicated to allow for generics.
 * </p>
 */
public class ObjectArrayComparator<T> implements Comparator<T[]> {
    @Override
    public int compare(final T[] o1, final T[] o2) {
        if (o1 == o2) {
            return 0;

        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }

        final int len = Math.min(o1.length, o2.length);
        for (int ii = 0; ii < len; ++ii) {
            final int cmp = ObjectComparisons.compare(o1[ii], o2[ii]);
            if (cmp != 0) {
                return cmp;
            }
        }

        return o1.length - o2.length;
    }
}
