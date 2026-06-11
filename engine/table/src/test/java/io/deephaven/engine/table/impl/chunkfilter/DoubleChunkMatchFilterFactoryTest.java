//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DoubleChunkMatchFilterFactoryTest {

    @Test
    public void zerosCanonicalized() {
        checkNonCanonicalsEqual(0.0d, -0.0d);
    }

    @Test
    public void nansCanonicalized() {
        final double nonCanonicalNaN = Double.longBitsToDouble(0xfff8000000000000L);
        checkNonCanonicalsEqual(Double.NaN, nonCanonicalNaN);
    }

    @Test
    public void testSetContains() {
        DoubleChunkFilter filter = DoubleChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, 1.0, 2.0, 3.0, 4.0);
        assertTrue(filter.matches(1.0));
        assertTrue(filter.matches(2.0));
        assertTrue(filter.matches(3.0));
        assertTrue(filter.matches(4.0));
        assertFalse(filter.matches(5.0));
        assertFalse(filter.matches(0.0));
        assertFalse(filter.matches(Double.NaN));
    }

    static void checkNonCanonicalsEqual(double x, double y) {
        assertNotEquals(
                Double.doubleToRawLongBits(x),
                Double.doubleToRawLongBits(y));
        assertEquals(
                DoubleChunkMatchFilterFactory.getBits(x),
                DoubleChunkMatchFilterFactory.getBits(y));
    }
}
