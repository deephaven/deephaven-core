//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AdaptiveOrderedLongSetBuilderRandomTest {

    @Test
    public void testAddRowSetOnFreshBuilder() {
        // A fresh builder has no inner builder yet; add(SortedRanges/RspBitmap) used to NPE.
        try (final WritableRowSet sortedRangesBacked = RowSetFactory.fromKeys(1, 5, 9, 100)) {
            final AdaptiveOrderedLongSetBuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();
            builder.addRowSet(sortedRangesBacked);
            final OrderedLongSet result = builder.getOrderedLongSet();
            assertEquals(4, result.ixCardinality());
            assertEquals(1, result.ixFirstKey());
            assertEquals(100, result.ixLastKey());
            result.ixRelease();
        }
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(3);
        rb = rb.addRange(1_000_000, 1_000_010);
        try (final WritableRowSet rspBacked = new WritableRowSetImpl(rb)) {
            final AdaptiveOrderedLongSetBuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();
            builder.addRowSet(rspBacked);
            final OrderedLongSet result = builder.getOrderedLongSet();
            assertEquals(12, result.ixCardinality());
            assertEquals(3, result.ixFirstKey());
            assertEquals(1_000_010, result.ixLastKey());
            result.ixRelease();
        }
    }

    @Test
    public void testAddRowSetAfterPendingState() {
        // Pending single-range and pending-SortedRanges states must be flushed into the inner builder.
        try (final WritableRowSet sortedRangesBacked = RowSetFactory.fromKeys(50, 60)) {
            final AdaptiveOrderedLongSetBuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();
            builder.addKey(1);
            builder.addRange(10, 12);
            builder.addRowSet(sortedRangesBacked);
            final OrderedLongSet result = builder.getOrderedLongSet();
            assertEquals(6, result.ixCardinality());
            assertEquals(1, result.ixFirstKey());
            assertEquals(60, result.ixLastKey());
            result.ixRelease();
        }
    }
}
