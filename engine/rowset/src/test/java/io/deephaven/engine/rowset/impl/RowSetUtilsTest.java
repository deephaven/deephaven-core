//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RowSetUtilsTest {

    @Test
    public void testRangeSearchHighKeysMidpointDoesNotOverflow() {
        // Row keys are legal up to Long.MAX_VALUE; a naive (begin + end) / 2 midpoint wraps for keys >= 2^62.
        final long begin = Long.MAX_VALUE / 2;
        final long end = Long.MAX_VALUE - 1;
        final long target = Long.MAX_VALUE - 5;
        final long result = RowSetUtils.rangeSearch(begin, end, value -> Long.compare(target, value));
        assertEquals(target, result);
    }
}
