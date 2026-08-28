//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import org.junit.Test;

import static org.junit.Assert.*;

public class FloatChunkMatchFilterFactoryTest {

    @Test
    public void zerosCanonicalized() {
        checkNonCanonicalsEqual(0.0f, -0.0f);
    }

    @Test
    public void nansCanonicalized() {
        final float nonCanonicalNaN = Float.intBitsToFloat(0xfff80000);
        checkNonCanonicalsEqual(Float.NaN, nonCanonicalNaN);
    }

    @Test
    public void testSetContains() {
        FloatChunkFilter f = FloatChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, 1.0f, 2.0f, 3.0f, 4.0f);
        assertTrue(f.matches(1.0f));
        assertTrue(f.matches(2.0f));
        assertTrue(f.matches(3.0f));
        assertTrue(f.matches(4.0f));
        assertFalse(f.matches(5.0f));
        assertFalse(f.matches(0.0f));
        assertFalse(f.matches(Float.NaN));
    }

    static void checkNonCanonicalsEqual(float x, float y) {
        assertNotEquals(
                Float.floatToRawIntBits(x),
                Float.floatToRawIntBits(y));
        assertEquals(
                FloatChunkMatchFilterFactory.getBits(x),
                FloatChunkMatchFilterFactory.getBits(y));
    }

    /**
     * With no NaN among the values there is nothing for NaN matching to do, so both forms of the filter must behave
     * identically -- including on NaN itself, on both zeros, and on the null sentinel. Every value count is covered,
     * since the factory returns a different filter for one, two, three and more values.
     */
    @Test
    public void nanMatchIrrelevantWithoutNaNValues() {
        final float[] allValues = {1.0f, 0.0f, -3.5f, 7.25f};
        final float[] probes = {1.0f, 0.0f, -0.0f, -3.5f, 7.25f, 2.0f, NULL_FLOAT,
                Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY};
        for (int count = 1; count <= allValues.length; ++count) {
            final float[] values = Arrays.copyOf(allValues, count);
            for (final boolean inverted : new boolean[] {false, true}) {
                final FloatChunkFilter nanMatching = FloatChunkMatchFilterFactory.makeFilter(
                        MatchOptions.builder().nanMatch(true).inverted(inverted).build(), values);
                final FloatChunkFilter plain = FloatChunkMatchFilterFactory.makeFilter(
                        MatchOptions.builder().nanMatch(false).inverted(inverted).build(), values);
                for (final float probe : probes) {
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
        final float[] allValues = {Float.NaN, 1.0f, 2.0f, 3.0f};
        for (int count = 1; count <= allValues.length; ++count) {
            final float[] values = Arrays.copyOf(allValues, count);
            final FloatChunkFilter nanMatching = FloatChunkMatchFilterFactory.makeFilter(
                    MatchOptions.builder().nanMatch(true).build(), values);
            final FloatChunkFilter plain = FloatChunkMatchFilterFactory.makeFilter(
                    MatchOptions.builder().nanMatch(false).build(), values);
            assertTrue("count=" + count, nanMatching.matches(Float.NaN));
            assertFalse("count=" + count, plain.matches(Float.NaN));
        }
    }
}
