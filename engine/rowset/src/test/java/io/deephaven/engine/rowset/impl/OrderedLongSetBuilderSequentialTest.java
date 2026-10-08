//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class OrderedLongSetBuilderSequentialTest {

    /**
     * Once the builder holds an {@link io.deephaven.engine.rowset.impl.rsp.RspBitmap}, ordered chunks and shifted sets
     * go straight into it, past the scalar appends' checks.
     */
    @Test
    public void testNegativeKeysRejectedInBitmapMode() {
        final OrderedLongSetBuilderSequential builder = new OrderedLongSetBuilderSequential();
        long key = 0;
        while (builder.rb == null) {
            builder.appendKey(key);
            key += 2;
        }
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(2)) {
            chunk.set(0, -4);
            chunk.set(1, -2);
            assertThrows(IllegalArgumentException.class, () -> builder.appendOrderedRowKeysChunk(chunk, 0, 2));
        }
        try (final WritableRowSet sparse = RowSetFactory.fromKeys(key, key + 2, key + 100_000)) {
            final OrderedLongSet inner = ((WritableRowSetImpl) sparse).getInnerSet();
            final long shift = -key - 1;
            assertThrows(IllegalArgumentException.class, () -> builder.appendOrderedLongSet(shift, inner));
        }
        try (final WritableRowSet high = RowSetFactory.fromKeys(1L << 40, Long.MAX_VALUE)) {
            final OrderedLongSet inner = ((WritableRowSetImpl) high).getInnerSet();
            // The first key shifts fine, but the last wraps past Long.MAX_VALUE.
            assertThrows(IllegalArgumentException.class, () -> builder.appendOrderedLongSet(65536, inner));
            final RspBitmapBuilderSequential rspBuilder = new RspBitmapBuilderSequential();
            rspBuilder.appendKey(0);
            assertThrows(IllegalArgumentException.class, () -> rspBuilder.appendOrderedLongSet(65536, inner));
        }
        builder.getOrderedLongSet().ixRelease();
    }

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
