//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OrderedLongSetBuilderSequentialTest {

    @Test
    public void testBuilderReleasesResultOnBuild() {
        // The SortedRanges branch of getOrderedLongSet used to keep its reference to the returned set;
        // later builder use would then mutate (or double-release) the set already handed out.
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        builder.appendKey(1);
        builder.appendKey(5);
        final OrderedLongSet first = builder.getOrderedLongSet();
        assertEquals(2, first.ixCardinality());
        builder.appendKey(100);
        final OrderedLongSet second = builder.getOrderedLongSet();
        assertEquals(2, first.ixCardinality());
        assertEquals(5, first.ixLastKey());
        first.ixValidate();
        assertEquals(1, second.ixCardinality());
        assertEquals(100, second.ixFirstKey());
        first.ixRelease();
        second.ixRelease();
    }

    @Test
    public void testBuilderSingleRangeBranchClearsPendingState() {
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        builder.appendRange(1, 3);
        final OrderedLongSet first = builder.getOrderedLongSet();
        assertEquals(3, first.ixCardinality());
        builder.appendKey(10);
        final OrderedLongSet second = builder.getOrderedLongSet();
        assertEquals(1, second.ixCardinality());
        assertEquals(10, second.ixFirstKey());
        first.ixRelease();
        second.ixRelease();
    }
}
