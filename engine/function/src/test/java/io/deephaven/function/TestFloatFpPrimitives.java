/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function;

import static io.deephaven.function.FloatFpPrimitives.*;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import junit.framework.TestCase;

public class TestFloatFpPrimitives extends TestCase {
    public void testIsNan(){
        assertTrue(isNaN(Float.NaN));
        assertFalse(isNaN((float)3.0));
    }

    public void testIsInf(){
        assertTrue(isInf(Float.POSITIVE_INFINITY));
        assertTrue(isInf(Float.NEGATIVE_INFINITY));
        assertFalse(isInf((float)3.0));
    }

    public void testIsNormal() {
        assertTrue(isNormal(0));
        assertTrue(isNormal(1));
        assertTrue(isNormal(-1));
        assertFalse(isNormal(Float.POSITIVE_INFINITY));
        assertFalse(isNormal(Float.NEGATIVE_INFINITY));
        assertFalse(isNormal(Float.NaN));
        assertFalse(isNormal(NULL_FLOAT));
    }

    public void testContainsNonNormal() {
        assertFalse(containsNonNormal((float)0, (float)0, (float)0));
        assertFalse(containsNonNormal((float)-1, (float)0, (float)1));
        assertTrue(containsNonNormal((float)0, (float)0, Float.NEGATIVE_INFINITY));
        assertTrue(containsNonNormal((float)0, (float)0, Float.POSITIVE_INFINITY));
        assertTrue(containsNonNormal((float)0, (float)0, Float.NaN));
        assertTrue(containsNonNormal((float)0, (float)0, NULL_FLOAT));
    }
}
