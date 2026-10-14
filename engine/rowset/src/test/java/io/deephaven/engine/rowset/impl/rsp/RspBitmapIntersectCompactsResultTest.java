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

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspBitmapOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesImplOf;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * An intersection can only shrink its receiver, so a bitmap intersection is checked for a result small enough to hold
 * in a cheaper representation, as a bitmap minus and the subindex operations are. Without that check the keys alone
 * would not decide the result's representation: intersecting with a {@link SingleRange} or {@link SortedRanges} answers
 * with the narrowest type that fits, so an argument holding the same keys as a bitmap would have answered with a bitmap
 * instead. A result too large to fit keeps its bitmap, and keeps the single reference it was made with.
 */
public class RspBitmapIntersectCompactsResultTest {

    private static final long BS = BLOCK_SIZE;

    /** Three well-separated blocks, so an argument can pick out any part of them. */
    private static RspBitmap receiver() {
        return rspBitmapOf(new long[] {10 * BS, 10 * BS + 100},
                new long[] {20 * BS, 20 * BS + 100},
                new long[] {30 * BS, 30 * BS + 100});
    }

    private static void assertResult(
            final OrderedLongSet result, final Class<?> expectedImpl, final String expected, final String what) {
        assertTrue(what + " implementation is " + result.getClass().getSimpleName() + ", wanted a "
                + expectedImpl.getSimpleName(), expectedImpl.isInstance(result));
        result.ixValidate(what);
        try (final WritableRowSet rs = new WritableRowSetImpl(result)) {
            assertEquals(what, expected, render(rangesOf(rs)));
        }
    }

    /** What is left is one contiguous range, which needs neither a bitmap nor a range array. */
    @Test
    public void testSingleContiguousResultBecomesASingleRange() {
        final OrderedLongSet result = receiver().ixIntersectOnNew(
                rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40}));
        assertResult(result, SingleRange.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40}))), "single contiguous result");
    }

    /** What is left is a few ranges across several blocks, which fit a SortedRanges. */
    @Test
    public void testFewRangesResultBecomesSortedRanges() {
        final OrderedLongSet result = receiver().ixIntersectOnNew(
                rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40},
                        new long[] {20 * BS + 5, 20 * BS + 9},
                        new long[] {30 * BS + 1, 30 * BS + 2}));
        assertResult(result, SortedRanges.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS + 20, 10 * BS + 40},
                        new long[] {20 * BS + 5, 20 * BS + 9},
                        new long[] {30 * BS + 1, 30 * BS + 2}))),
                "few ranges result");
    }

    /** Nothing overlaps, so nothing is left. */
    @Test
    public void testEmptyResultBecomesTheEmptySet() {
        assertSame("disjoint bitmap argument", OrderedLongSet.EMPTY,
                receiver().ixIntersectOnNew(rspBitmapOf(new long[] {50 * BS, 50 * BS + 10})));
        assertSame("overlapping blocks but no shared keys", OrderedLongSet.EMPTY,
                receiver().ixIntersectOnNew(rspBitmapOf(new long[] {10 * BS + 200, 10 * BS + 300})));
        assertSame("empty argument", OrderedLongSet.EMPTY, receiver().ixIntersectOnNew(OrderedLongSet.EMPTY));
    }

    /**
     * The point of the change: the same keys give the same result representation however the argument is backed.
     * Before, a bitmap argument alone answered with a bitmap.
     */
    @Test
    public void testResultTypeDoesNotDependOnTheArgumentsRepresentation() {
        final long lo = 10 * BS + 20;
        final long hi = 10 * BS + 40;
        final OrderedLongSet viaBitmap = receiver().ixIntersectOnNew(rspBitmapOf(new long[] {lo, hi}));
        final OrderedLongSet viaSortedRanges = receiver().ixIntersectOnNew(sortedRangesImplOf(new long[] {lo, hi}));
        final OrderedLongSet viaSingleRange = receiver().ixIntersectOnNew(SingleRange.make(lo, hi));
        assertEquals("a bitmap argument should answer with the same type as sorted ranges",
                viaSortedRanges.getClass(), viaBitmap.getClass());
        assertEquals("a bitmap argument should answer with the same type as a single range",
                viaSingleRange.getClass(), viaBitmap.getClass());
        final String expected = render(rangesOf(rspBitmapOf(new long[] {lo, hi})));
        assertResult(viaBitmap, SingleRange.class, expected, "via bitmap");
        assertResult(viaSortedRanges, SingleRange.class, expected, "via sorted ranges");
        assertResult(viaSingleRange, SingleRange.class, expected, "via single range");
    }

    /**
     * Two whole blocks are far too many keys for any cheaper representation, so the result stays a bitmap -- the
     * assertion on its type below is what catches a fixture that turned out to be compactable after all. It must also
     * come back with the one reference it was made with: releasing a result that was not replaced would hand back a
     * dead set.
     */
    @Test
    public void testLargeResultStaysABitmapAndKeepsItsReference() {
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS, 12 * BS - 1}, new long[] {20 * BS, 22 * BS - 1});
        final OrderedLongSet result = rb.ixIntersectOnNew(rspBitmapOf(new long[] {10 * BS, 22 * BS - 1}));
        assertEquals("a result that was kept must keep its reference", 1, result.ixRefCount());
        assertResult(result, RspBitmap.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS, 12 * BS - 1}, new long[] {20 * BS, 22 * BS - 1}))),
                "large result");
    }
}
