//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link RangePriorityQueueBuilder} merges a range with the one entered just before it when the two touch, rather than
 * queueing a second range. The adjacency test computes one past the earlier range's end, which for a range ending at
 * {@code Long.MAX_VALUE} wraps around and would leave the ranges unmerged. The queue is driven directly here: the
 * adaptive builder in front of it coalesces a handful of touching ranges on its own and never reaches this code.
 */
public class RowSetBuilderRandomAdjacentAtMaxTest {

    private static final long MAX = Long.MAX_VALUE;

    @Test
    public void testRangesTouchingAtTheMaximumKey() {
        final RangePriorityQueueBuilder builder = new RangePriorityQueueBuilder(16);
        builder.addRange(MAX - 5, MAX);
        builder.addRange(MAX - 10, MAX - 6);
        assertEquals("touching ranges merge", 1, builder.rangeCount());
        builder.addKey(MAX - 11);
        assertEquals("touching key merges", 1, builder.rangeCount());
        try (final WritableRowSet rs = new WritableRowSetImpl(builder.getOrderedLongSet())) {
            rs.validate();
            assertEquals(12, rs.size());
            assertTrue(rs.containsRange(MAX - 11, MAX));
        }
    }

    @Test
    public void testLoneKeyAtTheMaximumThenTheRangeBelowIt() {
        final RangePriorityQueueBuilder builder = new RangePriorityQueueBuilder(16);
        builder.addKey(MAX);
        builder.addRange(MAX - 3, MAX - 1);
        assertEquals("touching ranges merge", 1, builder.rangeCount());
        try (final WritableRowSet rs = new WritableRowSetImpl(builder.getOrderedLongSet())) {
            rs.validate();
            assertEquals(4, rs.size());
            assertTrue(rs.containsRange(MAX - 3, MAX));
        }
    }

    @Test
    public void testRangesNotTouchingAtTheMaximumKeyStayApart() {
        final RangePriorityQueueBuilder builder = new RangePriorityQueueBuilder(16);
        builder.addRange(MAX - 5, MAX);
        builder.addRange(MAX - 20, MAX - 10);
        assertEquals("a gap keeps the ranges apart", 2, builder.rangeCount());
        try (final WritableRowSet rs = new WritableRowSetImpl(builder.getOrderedLongSet())) {
            rs.validate();
            assertEquals(17, rs.size());
        }
    }
}
