//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspBitmapOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesImplOf;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Retaining in place narrows the receiver the same way {@code ixIntersectOnNew} narrows a new result, so a receiver
 * left holding a handful of ranges comes back as the type that fits them rather than as a bitmap.
 * <p>
 * The reference counting differs from the on-new case and is the delicate part. The set being narrowed may be the
 * caller's own, handed over by {@code getWriteRef}, so nothing here releases it: {@link WritableRowSetImpl#assign} is
 * what releases the set it replaces, and a release here as well would take the count to zero and then past it.
 */
public class RspBitmapRetainCompactsResultTest {

    private static final long BS = BLOCK_SIZE;

    private static RspBitmap receiver() {
        return rspBitmapOf(new long[] {10 * BS, 10 * BS + 100},
                new long[] {20 * BS, 20 * BS + 100},
                new long[] {30 * BS, 30 * BS + 100});
    }

    /** A bitmap argument narrows the receiver to one contiguous range, which no longer needs a bitmap. */
    @Test
    public void testSingleContiguousResultBecomesASingleRange() {
        final OrderedLongSet result = receiver().ixRetain(rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40}));
        assertTrue("implementation is " + result.getClass().getSimpleName(), result instanceof SingleRange);
        result.ixValidate("single contiguous result");
        try (final WritableRowSet rs = new WritableRowSetImpl(result)) {
            assertEquals("single contiguous result",
                    render(rangesOf(rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40}))), renderRanges(rs));
        }
    }

    /** What is left spans several blocks but still fits a SortedRanges. */
    @Test
    public void testFewRangesResultBecomesSortedRanges() {
        final OrderedLongSet result = receiver().ixRetain(
                rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40},
                        new long[] {20 * BS + 5, 20 * BS + 9},
                        new long[] {30 * BS + 1, 30 * BS + 2}));
        assertTrue("implementation is " + result.getClass().getSimpleName(), result instanceof SortedRanges);
        result.ixValidate("few ranges result");
    }

    /**
     * Through the public API, which is what makes the reference counting observable: {@code assign} releases the set it
     * replaces, so a release inside the retain as well would have taken the count past zero.
     */
    @Test
    public void testRetainThroughTheRowSetNarrowsTheBacking() {
        try (final WritableRowSet rs = new WritableRowSetImpl(receiver());
                final WritableRowSet other = new WritableRowSetImpl(
                        rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40}))) {
            assertBackedBy("before retain", rs, "RspBitmap");
            rs.retain(other);
            rs.validate();
            assertBackedBy("after retain", rs, "SingleRange");
            assertEquals("after retain", "655380-655400 ", renderRanges(rs));
            // The receiver is still usable afterwards, which a set released one time too many would not be.
            rs.insert(5);
            rs.validate();
            assertEquals("after a later insert", "5-5 655380-655400 ", renderRanges(rs));
        }
    }

    /** The narrowing must not depend on how the argument happens to be backed. */
    @Test
    public void testResultTypeDoesNotDependOnTheArgumentsRepresentation() {
        final long lo = 10 * BS + 20;
        final long hi = 10 * BS + 40;
        final OrderedLongSet viaBitmap = receiver().ixRetain(rspBitmapOf(new long[] {lo, hi}));
        final OrderedLongSet viaSortedRanges = receiver().ixRetain(sortedRangesImplOf(new long[] {lo, hi}));
        final OrderedLongSet viaSingleRange = receiver().ixRetain(SingleRange.make(lo, hi));
        assertEquals("a bitmap argument should narrow as sorted ranges do",
                viaSortedRanges.getClass(), viaBitmap.getClass());
        assertEquals("a bitmap argument should narrow as a single range does",
                viaSingleRange.getClass(), viaBitmap.getClass());
    }

    /**
     * Two whole blocks are far too many keys for any cheaper representation, so the receiver keeps its bitmap -- and
     * keeps its identity, which is what lets {@code assign} recognize that nothing was replaced.
     */
    @Test
    public void testLargeResultKeepsTheSameBitmap() {
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS, 12 * BS - 1}, new long[] {20 * BS, 22 * BS - 1});
        final int refsBefore = rb.ixRefCount();
        final OrderedLongSet result = rb.ixRetain(rspBitmapOf(new long[] {10 * BS, 22 * BS - 1}));
        assertSame("a result that cannot be narrowed must come back as the very same set", rb, result);
        assertEquals("and with its reference count untouched", refsBefore, rb.ixRefCount());
        result.ixValidate("large result");
    }

    /** Nothing overlaps, so the receiver empties. */
    @Test
    public void testEmptyResultBecomesTheEmptySet() {
        assertSame("disjoint blocks", OrderedLongSet.EMPTY,
                receiver().ixRetain(rspBitmapOf(new long[] {50 * BS, 50 * BS + 10})));
        assertSame("same block, no shared keys", OrderedLongSet.EMPTY,
                receiver().ixRetain(rspBitmapOf(new long[] {10 * BS + 200, 10 * BS + 300})));
    }

    /** Two blocks with a wide gap, so a window inside the gap passes the bounds checks and still keeps nothing. */
    private static RspBitmap receiverWithAGap() {
        return rspBitmapOf(new long[] {10 * BS, 10 * BS + 100}, new long[] {30 * BS, 30 * BS + 100});
    }

    /**
     * A shared receiver emptied by a retain. The copy {@code getWriteRef} made is reachable from nowhere once the empty
     * set is answered with, so it is released there rather than left with a count nobody will drop.
     */
    @Test
    public void testSharedReceiverEmptiedByRetain() {
        final RspBitmap rb = receiverWithAGap();
        final OrderedLongSet shared = rb.ixCowRef();
        assertEquals("two holders", 2, rb.ixRefCount());
        // Inside the gap: it overlaps our first-to-last span, so the disjoint check does not answer first.
        assertSame("nothing is kept", OrderedLongSet.EMPTY,
                rb.ixRetain(rspBitmapOf(new long[] {20 * BS, 20 * BS + 5})));
        assertEquals("the shared receiver keeps both references", 2, rb.ixRefCount());
        assertEquals("and the other holder sees them too", 2, shared.ixRefCount());
    }

    /** The same for a range window rather than a set argument. */
    @Test
    public void testSharedReceiverEmptiedByRetainRange() {
        final RspBitmap rb = receiverWithAGap();
        final OrderedLongSet shared = rb.ixCowRef();
        assertSame("nothing is kept", OrderedLongSet.EMPTY, rb.ixRetainRange(20 * BS, 20 * BS + 5));
        assertEquals("the shared receiver keeps both references", 2, rb.ixRefCount());
        assertEquals("and the other holder sees them too", 2, shared.ixRefCount());
    }

    /** An unshared receiver is narrowed in place, so there is no working copy to release. */
    @Test
    public void testUnsharedReceiverEmptiedByRetainRange() {
        final RspBitmap rb = receiverWithAGap();
        assertEquals("sole holder", 1, rb.ixRefCount());
        assertSame("nothing is kept", OrderedLongSet.EMPTY, rb.ixRetainRange(20 * BS, 20 * BS + 5));
    }

    /** A range window that trims an unshared receiver but leaves far too many keys to hold any other way. */
    @Test
    public void testUnsharedReceiverTrimmedByRetainRangeKeepsItsBitmap() {
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS, 12 * BS - 1}, new long[] {20 * BS, 22 * BS - 1});
        final OrderedLongSet result = rb.ixRetainRange(10 * BS, 21 * BS - 1);
        assertSame("a result that cannot be narrowed must come back as the very same set", rb, result);
        assertEquals("and with its reference count untouched", 1, rb.ixRefCount());
        assertEquals("trimmed", "655360-786431 1310720-1376255 ", renderRanges(new WritableRowSetImpl(result)));
        result.ixValidate("trimmed");
    }

    /** A shared receiver narrowed by a range window, which is the other place a working copy can be replaced. */
    @Test
    public void testSharedReceiverNarrowedByRetainRange() {
        final RspBitmap rb = receiver();
        final OrderedLongSet shared = rb.ixCowRef();
        final OrderedLongSet result = rb.ixRetainRange(10 * BS + 20, 10 * BS + 40);
        assertTrue("implementation is " + result.getClass().getSimpleName(), result instanceof SingleRange);
        assertNotSame("the narrowed result is a separate set", rb, result);
        assertEquals("the shared receiver keeps both references", 2, rb.ixRefCount());
        assertEquals("and the other holder sees them too", 2, shared.ixRefCount());
        result.ixValidate("narrowed by range");
    }

    /**
     * A shared receiver is copied by {@code getWriteRef} before being narrowed, so neither the original nor the copy
     * the other holder sees may have its count disturbed. {@code SortedRangesTest.testRetainRefCountRegress} makes the
     * same check for the sorted-ranges path.
     */
    @Test
    public void testSharedReceiverKeepsItsReferenceCount() {
        final RspBitmap rb = receiver();
        final OrderedLongSet shared = rb.ixCowRef();
        assertEquals("two holders", 2, rb.ixRefCount());
        final OrderedLongSet result = rb.ixRetain(rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40}));
        assertEquals("the shared receiver keeps both references", 2, rb.ixRefCount());
        assertEquals("and the other holder sees them too", 2, shared.ixRefCount());
        assertNotSame("the narrowed result is a separate set", rb, result);
        // The untouched holder still sees every key it started with.
        try (final WritableRowSet rs = new WritableRowSetImpl(shared)) {
            assertEquals("the other holder is unchanged", renderRanges(new WritableRowSetImpl(receiver())),
                    renderRanges(rs));
        }
    }
}
