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
 * A minus can only shrink its receiver, so a bitmap minus is checked for a result small enough to hold in a cheaper
 * representation. Left as a bitmap, a result that is now a handful of ranges would make every later operation on it pay
 * bitmap costs. A result too large to fit keeps its bitmap, and keeps the single reference it was made with.
 */
public class RspBitmapMinusCompactsResultTest {

    private static final long BS = BLOCK_SIZE;

    /** {@link SingleRange} and {@link SortedRanges} both pick a subtype by magnitude, so the family is what matters. */
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
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS + 5, 10 * BS + 40}, new long[] {20 * BS, 20 * BS + 30});
        final OrderedLongSet result = rb.ixMinusOnNew(SingleRange.make(20 * BS, 30 * BS));
        assertResult(result, SingleRange.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS + 5, 10 * BS + 40}))), "single contiguous result");
    }

    /** What is left is a few ranges, which fit a SortedRanges. The argument is a SortedRanges. */
    @Test
    public void testFewRangesResultFromSortedRangesArgument() {
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS, 10 * BS + 100},
                new long[] {20 * BS, 20 * BS + 100},
                new long[] {30 * BS, 30 * BS + 100});
        final OrderedLongSet result = rb.ixMinusOnNew(
                sortedRangesImplOf(new long[] {10 * BS + 50, 10 * BS + 60}, new long[] {20 * BS, 20 * BS + 100}));
        assertResult(result, SortedRanges.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS, 10 * BS + 49},
                        new long[] {10 * BS + 61, 10 * BS + 100},
                        new long[] {30 * BS, 30 * BS + 100}))),
                "few ranges from sorted ranges");
    }

    /** The same, reached through the bitmap-argument path rather than the sorted-ranges one. */
    @Test
    public void testFewRangesResultFromBitmapArgument() {
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS, 10 * BS + 100},
                new long[] {20 * BS, 20 * BS + 100},
                new long[] {30 * BS, 30 * BS + 100});
        final OrderedLongSet result = rb.ixMinusOnNew(
                rspBitmapOf(new long[] {10 * BS + 50, 10 * BS + 60}, new long[] {20 * BS, 20 * BS + 100}));
        assertResult(result, SortedRanges.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS, 10 * BS + 49},
                        new long[] {10 * BS + 61, 10 * BS + 100},
                        new long[] {30 * BS, 30 * BS + 100}))),
                "few ranges from bitmap");
    }

    /** Nothing is left. Both argument paths that can reach an empty result are taken. */
    @Test
    public void testEmptyResultBecomesTheEmptySet() {
        final long[][] ranges = {new long[] {10 * BS + 5, 10 * BS + 40}, new long[] {20 * BS, 20 * BS + 30}};
        final RspBitmap fromSortedRanges = rspBitmapOf(ranges);
        assertSame("emptied by sorted ranges", OrderedLongSet.EMPTY,
                fromSortedRanges.ixMinusOnNew(sortedRangesImplOf(ranges)));
        final RspBitmap fromBitmap = rspBitmapOf(ranges);
        assertSame("emptied by bitmap", OrderedLongSet.EMPTY, fromBitmap.ixMinusOnNew(rspBitmapOf(ranges)));
    }

    /**
     * The result is far too large for any cheaper representation, so it stays a bitmap. It must also come back with the
     * one reference it was made with: releasing a result that was not replaced would hand back a dead set.
     */
    @Test
    public void testLargeResultStaysABitmapAndKeepsItsReference() {
        final RspBitmap rb = rspBitmapOf(new long[] {10 * BS, 12 * BS - 1}, new long[] {20 * BS, 22 * BS - 1});
        final OrderedLongSet result = rb.ixMinusOnNew(SingleRange.make(11 * BS, 11 * BS + 5));
        assertTrue("the fixture must not fit a SortedRanges, or the test proves nothing",
                result.ixCardinality() > SortedRanges.LONG_DENSE_MAX_CAPACITY);
        assertEquals("a result that was kept must keep its reference", 1, result.ixRefCount());
        assertResult(result, RspBitmap.class,
                render(rangesOf(rspBitmapOf(new long[] {10 * BS, 11 * BS - 1},
                        new long[] {11 * BS + 6, 12 * BS - 1},
                        new long[] {20 * BS, 22 * BS - 1}))),
                "large result");
    }
}
