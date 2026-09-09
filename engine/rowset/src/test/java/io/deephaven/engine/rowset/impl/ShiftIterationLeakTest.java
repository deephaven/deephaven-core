//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Iteration helpers that create iterators internally must release them; an iterator over an RSP-backed row set holds an
 * acquired reference, so a leak leaves the set permanently shared and forces a copy on every later mutation.
 */
public class ShiftIterationLeakTest {

    private static RspBitmap makeRsp() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.add(5);
        rb = rb.addRange(BLOCK_SIZE, BLOCK_SIZE + 20);
        rb = rb.add(3 * BLOCK_SIZE + 7);
        rb.finishMutationsAndOptimize();
        return rb;
    }

    @Test
    public void testForAllInRowSetDoesNotLeakFilterRowSetReference() {
        final RspBitmap rb = makeRsp();
        final int before = rb.refCount();
        try (final WritableRowSet filter = new WritableRowSetImpl(rb.ixCowRef())) {
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 10, 100); // positive delta: uses the reverse iterator
            builder.shiftRange(BLOCK_SIZE, BLOCK_SIZE + 30, -1); // negative delta: uses the forward iterator
            final RowSetShiftData shiftData = builder.build();
            final long[] count = new long[1];
            shiftData.forAllInRowSet(filter, (key, delta) -> ++count[0]);
            assertTrue(count[0] > 0);
        }
        assertEquals(before, rb.refCount());
    }

    @Test
    public void testForAllInRowSetEarlyBreakDoesNotLeakFilterRowSetReference() {
        // Shift ranges that lie entirely below the filter's first key make both passes abandon their
        // iterators via the !advance(...) break, without draining them.
        final RspBitmap rb = makeRsp();
        final int before = rb.refCount();
        try (final WritableRowSet filter = new WritableRowSetImpl(rb.ixCowRef())) {
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 1, 1); // positive delta, below filter.firstRowKey() == 5
            final RowSetShiftData shiftData = builder.build();
            final long[] count = new long[1];
            shiftData.forAllInRowSet(filter, (key, delta) -> ++count[0]);
            assertEquals(0, count[0]);
        }
        assertEquals(before, rb.refCount());
    }

    @Test
    public void testForAllInvertedLongRangesDoesNotLeakSourceReference() {
        final RspBitmap rb = makeRsp();
        final int before = rb.refCount();
        try (final RowSet source = new WritableRowSetImpl(rb.ixCowRef());
                final RowSet dest = RowSetFactory.fromKeys(BLOCK_SIZE + 2, BLOCK_SIZE + 3, 3 * BLOCK_SIZE + 7)) {
            final long[] count = new long[1];
            RowSetUtils.forAllInvertedLongRanges(source, dest, (start, end) -> ++count[0]);
            assertTrue(count[0] > 0);
        }
        assertEquals(before, rb.refCount());
    }

    @Test
    public void testForAllInvertedLongRangesResultsAreCorrect() {
        // Guard the behavior while we are changing how the iterator is managed.
        try (final WritableRowSet source = RowSetFactory.fromRange(10, 19);
                final WritableRowSet dest = RowSetFactory.fromRange(18, 19)) {
            source.insertRange(30, 39);
            dest.insertRange(30, 31);
            final StringBuilder sb = new StringBuilder();
            RowSetUtils.forAllInvertedLongRanges(source, dest, (start, end) -> sb.append(start).append("-")
                    .append(end).append(";"));
            assertEquals("8-11;", sb.toString());
            try (final RowSet inverted = source.invert(dest)) {
                assertEquals(4, inverted.size());
                assertEquals(8, inverted.firstRowKey());
                assertEquals(11, inverted.lastRowKey());
            }
        }
    }

    @Test
    public void testRowSequenceIteratorAdvanceAndGetPositionDistanceStillWorks() {
        // forAllInvertedLongRanges relies on this; keep it pinned.
        try (final RowSet source = RowSetFactory.fromRange(10, 19);
                final RowSequence.Iterator it = source.getRowSequenceIterator()) {
            assertEquals(2, it.advanceAndGetPositionDistance(12));
            assertEquals(3, it.advanceAndGetPositionDistance(15));
        }
    }
}
