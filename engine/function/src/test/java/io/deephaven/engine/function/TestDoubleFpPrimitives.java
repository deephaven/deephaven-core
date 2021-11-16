/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestFloatFpPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.function;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import junit.framework.TestCase;

public class TestDoubleFpPrimitives extends TestCase {
    public void testIsNan(){
        assertTrue(DoubleFpPrimitives.isNaN(Double.NaN));
        assertFalse(DoubleFpPrimitives.isNaN((double)3.0));
    }

    public void testIsInf(){
        assertTrue(DoubleFpPrimitives.isInf(Double.POSITIVE_INFINITY));
        assertTrue(DoubleFpPrimitives.isInf(Double.NEGATIVE_INFINITY));
        assertFalse(DoubleFpPrimitives.isInf((double)3.0));
    }

    public void testIsNormal() {
        assertTrue(DoubleFpPrimitives.isNormal(0));
        assertTrue(DoubleFpPrimitives.isNormal(1));
        assertTrue(DoubleFpPrimitives.isNormal(-1));
        assertFalse(DoubleFpPrimitives.isNormal(Double.POSITIVE_INFINITY));
        assertFalse(DoubleFpPrimitives.isNormal(Double.NEGATIVE_INFINITY));
        assertFalse(DoubleFpPrimitives.isNormal(Double.NaN));
        assertFalse(DoubleFpPrimitives.isNormal(NULL_DOUBLE));
    }

    public void testContainsNonNormal() {
        assertFalse(DoubleFpPrimitives.containsNonNormal((double)0, (double)0, (double)0));
        assertFalse(DoubleFpPrimitives.containsNonNormal((double)-1, (double)0, (double)1));
        assertTrue(DoubleFpPrimitives.containsNonNormal((double)0, (double)0, Double.NEGATIVE_INFINITY));
        assertTrue(DoubleFpPrimitives.containsNonNormal((double)0, (double)0, Double.POSITIVE_INFINITY));
        assertTrue(DoubleFpPrimitives.containsNonNormal((double)0, (double)0, Double.NaN));
        assertTrue(DoubleFpPrimitives.containsNonNormal((double)0, (double)0, NULL_DOUBLE));
    }
}
