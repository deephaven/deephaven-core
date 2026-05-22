//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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

    static void checkNonCanonicalsEqual(double x, double y) {
        assertNotEquals(
                Double.doubleToRawLongBits(x),
                Double.doubleToRawLongBits(y));
        assertEquals(
                DoubleChunkMatchFilterFactory.getBits(x),
                DoubleChunkMatchFilterFactory.getBits(y));
    }
}
