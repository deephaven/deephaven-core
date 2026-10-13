//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TestAllocateOutputPosition {
    @Test
    public void testAllocatesInOrder() {
        final MutableInt next = new MutableInt(0);
        assertEquals(0, ChunkedOperatorAggregationHelper.allocateOutputPosition(next));
        assertEquals(1, ChunkedOperatorAggregationHelper.allocateOutputPosition(next));
        assertEquals(2, next.get());
    }

    @Test
    public void testFailsRatherThanWrapping() {
        final MutableInt next = new MutableInt(Integer.MAX_VALUE - 1);
        assertEquals(Integer.MAX_VALUE - 1, ChunkedOperatorAggregationHelper.allocateOutputPosition(next));
        final UnsupportedOperationException thrown = assertThrows(UnsupportedOperationException.class,
                () -> ChunkedOperatorAggregationHelper.allocateOutputPosition(next));
        assertEquals("Aggregation output positions exhausted: 2147483647 states have been created",
                thrown.getMessage());
    }
}
