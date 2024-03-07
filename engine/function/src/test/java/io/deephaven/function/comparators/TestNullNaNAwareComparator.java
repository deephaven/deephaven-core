//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function.comparators;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.Comparator;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * Test NullNaNAwareComparator.
 */
public class TestNullNaNAwareComparator extends BaseArrayTestCase {

    public void testDouble() {
        final Double v1 = 1.4;
        final Double v2 = 2.3;
        final Double v3 = NULL_DOUBLE;
        final Double v4 = NULL_DOUBLE;
        final Double v5 = null;
        final Double v6 = Double.NaN;
        final Double v7 = Double.NaN;

        final Comparator<Double> cmp = new NullNaNAwareComparator<>();
        assertEquals(0, cmp.compare(v1, v1));
        assertEquals(0, cmp.compare(v2, v2));
        assertEquals(0, cmp.compare(v3, v3));
        assertEquals(0, cmp.compare(v4, v4));
        assertEquals(0, cmp.compare(v5, v5));
        assertEquals(0, cmp.compare(v6, v6));
        assertEquals(0, cmp.compare(v7, v7));

        assertEquals(0, cmp.compare(v3, v4));
        assertEquals(0, cmp.compare(v4, v3));
        assertEquals(0, cmp.compare(v3, v5));
        assertEquals(0, cmp.compare(v5, v3));
        assertEquals(0, cmp.compare(v4, v5));
        assertEquals(0, cmp.compare(v5, v4));

        assertEquals(0, cmp.compare(v6, v7));
        assertEquals(0, cmp.compare(v7, v6));

        assertEquals(-1, cmp.compare(v1, v2));
        assertEquals(1, cmp.compare(v2, v1));

        assertEquals(1, cmp.compare(v1, v3));
        assertEquals(-1, cmp.compare(v3, v1));
        assertEquals(1, cmp.compare(v1, v5));
        assertEquals(-1, cmp.compare(v5, v1));

        assertEquals(-1, cmp.compare(v1, v6));
        assertEquals(1, cmp.compare(v6, v1));
    }

    public void testFloat() {
        final Float v1 = 1.4f;
        final Float v2 = 2.3f;
        final Float v3 = NULL_FLOAT;
        final Float v4 = NULL_FLOAT;
        final Float v5 = null;
        final Float v6 = Float.NaN;
        final Float v7 = Float.NaN;

        final Comparator<Float> cmp = new NullNaNAwareComparator<>();
        assertEquals(0, cmp.compare(v1, v1));
        assertEquals(0, cmp.compare(v2, v2));
        assertEquals(0, cmp.compare(v3, v3));
        assertEquals(0, cmp.compare(v4, v4));
        assertEquals(0, cmp.compare(v5, v5));
        assertEquals(0, cmp.compare(v6, v6));
        assertEquals(0, cmp.compare(v7, v7));

        assertEquals(0, cmp.compare(v3, v4));
        assertEquals(0, cmp.compare(v4, v3));
        assertEquals(0, cmp.compare(v3, v5));
        assertEquals(0, cmp.compare(v5, v3));
        assertEquals(0, cmp.compare(v4, v5));
        assertEquals(0, cmp.compare(v5, v4));

        assertEquals(0, cmp.compare(v6, v7));
        assertEquals(0, cmp.compare(v7, v6));

        assertEquals(-1, cmp.compare(v1, v2));
        assertEquals(1, cmp.compare(v2, v1));

        assertEquals(1, cmp.compare(v1, v3));
        assertEquals(-1, cmp.compare(v3, v1));
        assertEquals(1, cmp.compare(v1, v5));
        assertEquals(-1, cmp.compare(v5, v1));

        assertEquals(-1, cmp.compare(v1, v6));
        assertEquals(1, cmp.compare(v6, v1));
    }
}
