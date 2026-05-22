//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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

    static void checkNonCanonicalsEqual(float x, float y) {
        assertNotEquals(
                Float.floatToRawIntBits(x),
                Float.floatToRawIntBits(y));
        assertEquals(
                FloatChunkMatchFilterFactory.getBits(x),
                FloatChunkMatchFilterFactory.getBits(y));
    }
}
