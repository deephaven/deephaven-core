//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspBitmapOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesImplOf;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * A minus whose argument lies wholly outside the bitmap's own span of keys cannot change it, so the answer is a
 * reference to the bitmap rather than a copy of it. The tests pin the sharing itself, not just the content: a copy
 * gives the right keys too, and the whole point of the check is to avoid making one. They also pin the boundary, where
 * an argument that merely touches the bitmap's first or last key is not disjoint and must still be removed.
 */
public class RspBitmapMinusDisjointArgumentTest {

    private static final long BS = BLOCK_SIZE;

    /** A bitmap over three well-separated blocks, so all three representations can sit clear of it on either side. */
    private static RspBitmap receiver() {
        return rspBitmapOf(new long[] {10 * BS + 5, 10 * BS + 40},
                new long[] {20 * BS, 22 * BS - 1},
                new long[] {30 * BS + 7, 30 * BS + 7});
    }

    /** The argument cannot change the receiver, so the receiver itself comes back, with a reference taken for it. */
    private static void assertShared(final OrderedLongSet other, final String what) {
        final RspBitmap rb = receiver();
        final String before = render(rangesOf(rb));
        final int refsBefore = rb.ixRefCount();
        final OrderedLongSet result = rb.ixMinusOnNew(other);
        assertSame(what + " should share rather than copy", rb, result);
        assertEquals(what + " should take a reference", refsBefore + 1, rb.ixRefCount());
        assertEquals(what + " content", before, render(rangesOf(rb)));
        result.ixRelease();
        assertEquals(what + " reference released", refsBefore, rb.ixRefCount());
    }

    /** The argument does reach the receiver, so a fresh set comes back holding {@code expected}. */
    private static void assertRemoved(final OrderedLongSet other, final String expected, final String what) {
        final RspBitmap rb = receiver();
        final int refsBefore = rb.ixRefCount();
        final OrderedLongSet result = rb.ixMinusOnNew(other);
        assertNotSame(what + " should not share", rb, result);
        assertEquals(what + " should leave the receiver's reference count alone", refsBefore, rb.ixRefCount());
        result.ixValidate(what);
        try (final WritableRowSet rs = new WritableRowSetImpl(result)) {
            assertEquals(what, expected, render(rangesOf(rs)));
        }
    }

    /** The degenerate case of the same rule: an empty argument removes nothing, so it too shares. */
    @Test
    public void testEmptyArgument() {
        assertShared(OrderedLongSet.EMPTY, "empty argument");
    }

    /**
     * The mirror degenerate case: an empty receiver has nothing to lose whatever the argument is. The bounds check
     * reads the receiver's spans directly, so an empty one has to be answered before it, not by it.
     */
    @Test
    public void testEmptyReceiver() {
        assertSame("empty receiver, single range", OrderedLongSet.EMPTY,
                RspBitmap.makeEmpty().ixMinusOnNew(SingleRange.make(10, 20)));
        assertSame("empty receiver, sorted ranges", OrderedLongSet.EMPTY,
                RspBitmap.makeEmpty().ixMinusOnNew(sortedRangesImplOf(new long[] {10, 20})));
        assertSame("empty receiver, bitmap", OrderedLongSet.EMPTY,
                RspBitmap.makeEmpty().ixMinusOnNew(rspBitmapOf(new long[] {10, 20})));
        assertSame("empty receiver, empty argument", OrderedLongSet.EMPTY,
                RspBitmap.makeEmpty().ixMinusOnNew(OrderedLongSet.EMPTY));
    }

    @Test
    public void testSingleRangePastTheEnd() {
        assertShared(SingleRange.make(40 * BS, 41 * BS), "single range past the end");
    }

    @Test
    public void testSingleRangeBeforeTheStart() {
        assertShared(SingleRange.make(0, 9 * BS), "single range before the start");
    }

    @Test
    public void testSortedRangesPastTheEnd() {
        assertShared(sortedRangesImplOf(new long[] {40 * BS, 40 * BS + 3}, new long[] {50 * BS, 50 * BS + 3}),
                "sorted ranges past the end");
    }

    @Test
    public void testSortedRangesBeforeTheStart() {
        assertShared(sortedRangesImplOf(new long[] {1, 5}, new long[] {9 * BS, 9 * BS + 3}),
                "sorted ranges before the start");
    }

    @Test
    public void testBitmapPastTheEnd() {
        assertShared(rspBitmapOf(new long[] {40 * BS, 40 * BS + 3}, new long[] {60 * BS, 62 * BS - 1}),
                "bitmap past the end");
    }

    @Test
    public void testBitmapBeforeTheStart() {
        assertShared(rspBitmapOf(new long[] {0, 3}, new long[] {9 * BS, 9 * BS + 3}), "bitmap before the start");
    }

    /**
     * The argument starts on the receiver's very last key, one short of disjoint. A check written with {@code <=} would
     * share here and silently keep a key it was asked to remove.
     */
    @Test
    public void testArgumentStartingOnTheLastKey() {
        final String expected = render(rangesOf(rspBitmapOf(new long[] {10 * BS + 5, 10 * BS + 40},
                new long[] {20 * BS, 22 * BS - 1})));
        assertRemoved(SingleRange.make(30 * BS + 7, 40 * BS), expected, "argument starting on the last key");
    }

    /** The mirror of the above: the argument ends on the receiver's very first key. */
    @Test
    public void testArgumentEndingOnTheFirstKey() {
        final String expected = render(rangesOf(rspBitmapOf(new long[] {10 * BS + 6, 10 * BS + 40},
                new long[] {20 * BS, 22 * BS - 1},
                new long[] {30 * BS + 7, 30 * BS + 7})));
        assertRemoved(SingleRange.make(0, 10 * BS + 5), expected, "argument ending on the first key");
    }

    /** Neither side of the check applies: the argument overlaps, and the ordinary paths run. */
    @Test
    public void testOverlappingArgumentIsNotShared() {
        final String expected = render(rangesOf(rspBitmapOf(new long[] {10 * BS + 5, 10 * BS + 40},
                new long[] {30 * BS + 7, 30 * BS + 7})));
        assertRemoved(SingleRange.make(20 * BS, 22 * BS - 1), expected, "overlapping single range");
        assertRemoved(sortedRangesImplOf(new long[] {20 * BS, 22 * BS - 1}), expected, "overlapping sorted ranges");
        assertRemoved(rspBitmapOf(new long[] {20 * BS, 22 * BS - 1}), expected, "overlapping bitmap");
    }
}
