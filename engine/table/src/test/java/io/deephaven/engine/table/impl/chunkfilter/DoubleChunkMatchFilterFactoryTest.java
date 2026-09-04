//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
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

    /**
     * With no NaN among the values there is nothing for NaN matching to do, so both forms of the filter must behave
     * identically -- including on NaN itself, on both zeros, and on the null sentinel. Every value count is covered,
     * since the factory returns a different filter for one, two, three and more values.
     */
    @Test
    public void nanMatchIrrelevantWithoutNaNValues() {
        final double[] allValues = {1.0, 0.0, -3.5, 7.25};
        final double[] probes = {1.0, 0.0, -0.0, -3.5, 7.25, 2.0, NULL_DOUBLE,
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        for (int count = 1; count <= allValues.length; ++count) {
            final double[] values = Arrays.copyOf(allValues, count);
            for (final boolean inverted : new boolean[] {false, true}) {
                final DoubleChunkFilter nanMatching = DoubleChunkMatchFilterFactory.makeFilter(
                        MatchOptions.builder().nanMatch(true).inverted(inverted).build(), values);
                final DoubleChunkFilter plain = DoubleChunkMatchFilterFactory.makeFilter(
                        MatchOptions.builder().nanMatch(false).inverted(inverted).build(), values);
                for (final double probe : probes) {
                    assertEquals("count=" + count + " inverted=" + inverted + " probe=" + probe,
                            plain.matches(probe), nanMatching.matches(probe));
                }
            }
        }
    }

    /**
     * With NaN among the values, NaN matching is exactly what separates the two forms: it holds NaN equal to itself,
     * while without it NaN matches nothing.
     */
    @Test
    public void nanMatchDistinguishesNaNValues() {
        final double[] allValues = {Double.NaN, 1.0, 2.0, 3.0};
        for (int count = 1; count <= allValues.length; ++count) {
            final double[] values = Arrays.copyOf(allValues, count);
            final DoubleChunkFilter nanMatching = DoubleChunkMatchFilterFactory.makeFilter(
                    MatchOptions.builder().nanMatch(true).build(), values);
            final DoubleChunkFilter plain = DoubleChunkMatchFilterFactory.makeFilter(
                    MatchOptions.builder().nanMatch(false).build(), values);
            assertTrue("count=" + count, nanMatching.matches(Double.NaN));
            assertFalse("count=" + count, plain.matches(Double.NaN));
        }
    }
}
