//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayComparator and run "./gradlew replicateArrayComparators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.comparators;

import io.deephaven.util.compare.FloatComparisons;

import java.util.Comparator;

/**
 * Lexicographicaly compares two arrays using Deephaven ordering for the elements.
 */
public class FloatArrayComparator implements Comparator<float[]> {
    @Override
    public int compare(final float[] o1, final float[] o2) {
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
            final int cmp = FloatComparisons.compare(o1[ii], o2[ii]);
            if (cmp != 0) {
                return cmp;
            }
        }

        return o1.length - o2.length;
    }
}
