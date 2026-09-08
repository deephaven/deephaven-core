//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.engine.table.MatchOptions;
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
}
