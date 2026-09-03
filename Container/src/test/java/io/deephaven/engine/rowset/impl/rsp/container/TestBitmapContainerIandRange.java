//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.container.ContainerTestCommon.valuesOf;
import static org.junit.Assert.assertEquals;

/**
 * {@code iandRange} may edit a BitmapContainer in place. Values outside the retained range have to be cleared, not just
 * left out of the recomputed cardinality, or the container reports a size that disagrees with what it contains.
 */
public class TestBitmapContainerIandRange {

    /** Enough values to stay a BitmapContainer, plus one low outlier well before the range we will retain. */
    private static BitmapContainer withOutlierBelow(final int rangeStart, final int rangeEnd) {
        final BitmapContainer c = new BitmapContainer(new long[1 << 10], 0);
        c.iset((short) 2);
        for (int v = rangeStart; v <= rangeEnd; v += 2) {
            c.iset((short) v);
        }
        return c;
    }

    @Test
    public void testIandRangeClearsValuesBelowTheRange() {
        final int lo = 20000, hi = 36382;
        final BitmapContainer c = withOutlierBelow(lo, hi);
        final int expectedCardinality = c.getCardinality() - 1; // everything but the outlier at 2

        final Container result = c.iandRange(lo, hi + 1);

        final List<Integer> values = valuesOf(result);
        assertEquals("the value below the range must be gone", expectedCardinality, values.size());
        assertEquals("cardinality must agree with the contents", values.size(), result.getCardinality());
        assertEquals("the outlier must not be the first value", Integer.valueOf(lo), values.get(0));
    }

    @Test
    public void testIandRangeClearsValuesAboveTheRange() {
        final BitmapContainer c = new BitmapContainer(new long[1 << 10], 0);
        for (int v = 1000; v <= 17000; v += 2) {
            c.iset((short) v);
        }
        c.iset((short) 60000); // well above the range we retain
        final int expectedCardinality = c.getCardinality() - 1;

        final Container result = c.iandRange(1000, 17001);

        final List<Integer> values = valuesOf(result);
        assertEquals("the value above the range must be gone", expectedCardinality, values.size());
        assertEquals("cardinality must agree with the contents", values.size(), result.getCardinality());
        assertEquals("the outlier must not be the last value", Integer.valueOf(17000), values.get(values.size() - 1));
    }

    /** A retained range inside a single word, with outliers on both sides. */
    @Test
    public void testIandRangeSingleWordRange() {
        final BitmapContainer c = new BitmapContainer(new long[1 << 10], 0);
        for (int v = 5000; v <= 21000; v += 2) {
            c.iset((short) v);
        }
        c.iset((short) 3);
        c.iset((short) 40000);

        final Container result = c.iandRange(10000, 10032);

        final List<Integer> values = valuesOf(result);
        assertEquals("cardinality must agree with the contents", values.size(), result.getCardinality());
        for (final int v : values) {
            if (v < 10000 || v >= 10032) {
                throw new AssertionError("value outside the retained range survived: " + v);
            }
        }
    }
}
