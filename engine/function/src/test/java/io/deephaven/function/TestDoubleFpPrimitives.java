/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestFloatFpPrimitives and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import static io.deephaven.function.DoubleFpPrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import junit.framework.TestCase;

public class TestDoubleFpPrimitives extends TestCase {
    public void testIsNan(){
        assertTrue(isNaN(Double.NaN));
        assertFalse(isNaN((double)3.0));
    }

    public void testIsInf(){
        assertTrue(isInf(Double.POSITIVE_INFINITY));
        assertTrue(isInf(Double.NEGATIVE_INFINITY));
        assertFalse(isInf((double)3.0));
    }

    public void testIsNormal() {
        assertTrue(isNormal(0));
        assertTrue(isNormal(1));
        assertTrue(isNormal(-1));
        assertFalse(isNormal(Double.POSITIVE_INFINITY));
        assertFalse(isNormal(Double.NEGATIVE_INFINITY));
        assertFalse(isNormal(Double.NaN));
        assertFalse(isNormal(NULL_DOUBLE));
    }

    public void testContainsNonNormal() {
        assertFalse(containsNonNormal((double)0, (double)0, (double)0));
        assertFalse(containsNonNormal((double)-1, (double)0, (double)1));
        assertTrue(containsNonNormal((double)0, (double)0, Double.NEGATIVE_INFINITY));
        assertTrue(containsNonNormal((double)0, (double)0, Double.POSITIVE_INFINITY));
        assertTrue(containsNonNormal((double)0, (double)0, Double.NaN));
        assertTrue(containsNonNormal((double)0, (double)0, NULL_DOUBLE));
    }
}
