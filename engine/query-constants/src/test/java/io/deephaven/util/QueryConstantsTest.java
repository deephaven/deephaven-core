//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class QueryConstantsTest {

    @Test
    public void testMinFiniteFloat() {
        final float calculated = Math.nextUp(-Float.MAX_VALUE);
        // noinspection SimplifiableAssertion
        assertTrue(calculated == QueryConstants.MIN_FINITE_FLOAT);
        assertEquals(
                Float.floatToIntBits(calculated),
                Float.floatToIntBits(QueryConstants.MIN_FINITE_FLOAT));
    }

    @Test
    public void testMinFiniteDouble() {
        final double calculated = Math.nextUp(-Double.MAX_VALUE);
        // noinspection SimplifiableAssertion
        assertTrue(calculated == QueryConstants.MIN_FINITE_DOUBLE);
        assertEquals(
                Double.doubleToLongBits(calculated),
                Double.doubleToLongBits(QueryConstants.MIN_FINITE_DOUBLE));
    }
}
