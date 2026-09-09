//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OrderedLongSetBuilderSequentialTest {

    @Test
    public void testBuildIsSingleUse() {
        // SortedRanges result branch: the second build must fail rather than return a bogus result, and
        // must not have disturbed the set already returned.
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        builder.appendKey(1);
        builder.appendKey(5);
        final OrderedLongSet result = builder.getOrderedLongSet();
        assertEquals(2, result.ixCardinality());
        try {
            builder.getOrderedLongSet();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
        assertEquals(2, result.ixCardinality());
        assertEquals(5, result.ixLastKey());
        result.ixValidate();
        result.ixRelease();

        // SingleRange result branch.
        final OrderedLongSetBuilderSequential builder2 = new OrderedLongSetBuilderSequential();
        builder2.appendRange(1, 3);
        final OrderedLongSet single = builder2.getOrderedLongSet();
        assertEquals(3, single.ixCardinality());
        try {
            builder2.getOrderedLongSet();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
        single.ixRelease();

        // Empty result branch.
        final OrderedLongSetBuilderSequential builder3 = new OrderedLongSetBuilderSequential();
        assertEquals(OrderedLongSet.EMPTY, builder3.getOrderedLongSet());
        try {
            builder3.getOrderedLongSet();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testPublicBuildersAreSingleUse() {
        final RowSetBuilderSequential sequential = RowSetFactory.builderSequential();
        sequential.appendKey(7);
        try (final WritableRowSet rowSet = sequential.build()) {
            assertEquals(1, rowSet.size());
        }
        try {
            sequential.build();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }

        final RowSetBuilderRandom random = RowSetFactory.builderRandom();
        random.addKey(7);
        try (final WritableRowSet rowSet = random.build()) {
            assertEquals(1, rowSet.size());
        }
        try {
            random.build();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
        }
    }
}
