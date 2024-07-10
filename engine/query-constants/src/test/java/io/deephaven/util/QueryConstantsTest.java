//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class QueryConstantsTest {

    @Test
    public void testMinFiniteFloat() {
        assertEquals(Math.nextUp(-Float.MAX_VALUE), QueryConstants.MIN_FINITE_FLOAT, 0);
    }

    @Test
    public void testMinFiniteDouble() {
        assertEquals(Math.nextUp(-Double.MAX_VALUE), QueryConstants.MIN_FINITE_DOUBLE, 0);
    }
}
