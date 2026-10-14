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
     * while without it NaN matches nothing. NaN has to be the <em>only</em> probe they answer differently, so the
     * ordinary values are checked alongside it: dropping the NaN must leave the rest of the value set matching as it
     * did, and must not draw in a value that was never listed.
     */
    @Test
    public void nanMatchDistinguishesNaNValues() {
        final double[] allValues = {Double.NaN, 1.0, 2.0, 3.0};
        final double nonMember = 4.0;
        for (int count = 1; count <= allValues.length; ++count) {
            final double[] values = Arrays.copyOf(allValues, count);
            for (final boolean inverted : new boolean[] {false, true}) {
                final DoubleChunkFilter nanMatching = DoubleChunkMatchFilterFactory.makeFilter(
                        MatchOptions.builder().nanMatch(true).inverted(inverted).build(), values);
                final DoubleChunkFilter plain = DoubleChunkMatchFilterFactory.makeFilter(
                        MatchOptions.builder().nanMatch(false).inverted(inverted).build(), values);
                final String message = "count=" + count + " inverted=" + inverted;
                // NaN is listed, so NaN matching selects it and the plain form does not.
                assertEquals(message, !inverted, nanMatching.matches(Double.NaN));
                assertEquals(message, inverted, plain.matches(Double.NaN));
                // Every listed value other than NaN is selected by both forms alike.
                for (int ii = 1; ii < count; ++ii) {
                    final String valueMessage = message + " value=" + allValues[ii];
                    assertEquals(valueMessage, !inverted, nanMatching.matches(allValues[ii]));
                    assertEquals(valueMessage, !inverted, plain.matches(allValues[ii]));
                }
                // A value that was never listed is selected by neither.
                assertEquals(message, inverted, nanMatching.matches(nonMember));
                assertEquals(message, inverted, plain.matches(nonMember));
            }
        }
    }
}
