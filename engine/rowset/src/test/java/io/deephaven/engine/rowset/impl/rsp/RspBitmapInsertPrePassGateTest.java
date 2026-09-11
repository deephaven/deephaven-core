//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.RowSetTestCommon;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.shiftRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesImplOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.unionRanges;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Inserting a {@link SortedRanges} into an {@link RspBitmap} makes room for new spans ahead of time only when that can
 * pay off: above {@link RspBitmap#PARTIAL_BLOCK_PREPASS_MIN_SPANS} spans, and only if some block the ranges touch has
 * no span yet. These cover the two ways it is skipped, the test that decides the second, and the search fast path the
 * insert leans on when consecutive ranges share a block.
 */
public class RspBitmapInsertPrePassGateTest {

    private static final long BS = BLOCK_SIZE;
    private static final int OVER_GATE = RspBitmap.PARTIAL_BLOCK_PREPASS_MIN_SPANS + 50;

    /** A singleton span in every block from {@code firstBlock} for {@code blocks} blocks. */
    private static RspBitmap singletonsAtEveryBlock(final long firstBlock, final int blocks) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < blocks; ++i) {
            rb = rb.appendUnsafe((firstBlock + i) * BS + 7);
        }
        rb.finishMutations();
        return rb;
    }

    private static List<long[]> rangesOf(final SortedRanges sr) {
        final List<long[]> out = new ArrayList<>();
        sr.forEachLongRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    private static void checkInsert(final RspBitmap receiver, final SortedRanges sr, final String what) {
        final String expected = render(unionRanges(RowSetTestCommon.rangesOf(receiver), rangesOf(sr)));
        final RspBitmap w = receiver.deepCopy();
        w.insertOrderedLongSetUnsafeNoWriteCheck(sr);
        w.finishMutations();
        w.validate(what);
        assertEquals(what, expected, render(RowSetTestCommon.rangesOf(w)));
    }

    @Test
    public void testEveryBlockPresentWhenSpansAreContiguous() {
        final RspBitmap rb = singletonsAtEveryBlock(0, 10);
        assertTrue(rb.hasSpanForEveryBlockBetween(2 * BS + 1, 7 * BS + 1));
        assertTrue(rb.hasSpanForEveryBlockBetween(0, 10 * BS - 1));
        assertTrue(rb.hasSpanForEveryBlockBetween(3 * BS, 3 * BS + 100));
    }

    @Test
    public void testMissingBlockIsDetectedWhereverItFalls() {
        RspBitmap rb = singletonsAtEveryBlock(0, 5); // blocks 0..4
        for (int i = 6; i < 10; ++i) { // blocks 6..9; block 5 is missing
            rb = rb.appendUnsafe(i * BS + 7);
        }
        rb.finishMutations();
        assertFalse("gap inside", rb.hasSpanForEveryBlockBetween(2 * BS, 8 * BS));
        assertFalse("first block missing", rb.hasSpanForEveryBlockBetween(5 * BS, 8 * BS));
        assertFalse("last block missing", rb.hasSpanForEveryBlockBetween(2 * BS, 5 * BS));
        assertFalse("only block missing", rb.hasSpanForEveryBlockBetween(5 * BS + 1, 5 * BS + 2));
        assertFalse("past our end", rb.hasSpanForEveryBlockBetween(8 * BS, 12 * BS));
        assertFalse("before our start", rb.hasSpanForEveryBlockBetween(0, 12 * BS));
    }

    @Test
    public void testFullBlockSpans() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0, 4 * BS - 1); // full block span over blocks 0..3
        rb = rb.appendUnsafe(4 * BS + 7);
        rb = rb.appendUnsafe(5 * BS + 7);
        rb.finishMutations();
        assertTrue("both ends in the full block span", rb.hasSpanForEveryBlockBetween(BS + 1, 2 * BS + 1));
        assertTrue("whole full block span", rb.hasSpanForEveryBlockBetween(0, 4 * BS - 1));
        assertTrue("singletons only", rb.hasSpanForEveryBlockBetween(4 * BS, 5 * BS));
        // The span count under-counts the blocks a multi-block span covers, so this window reads as incomplete even
        // though it is not; the insert then makes its full pass, and finds nothing to do.
        assertFalse(rb.hasSpanForEveryBlockBetween(BS, 5 * BS));
    }

    /** Below the gate every insert skips the pass; this covers ranges in blocks we have, lack, and beyond our end. */
    @Test
    public void testInsertBelowGate() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < 20; i += 2) {
            rb = rb.appendUnsafe(i * BS + 7);
        }
        rb.finishMutations();
        checkInsert(rb, sortedRangesImplOf(new long[] {2 * BS + 1, 2 * BS + 3}, new long[] {3 * BS + 1, 3 * BS + 3},
                new long[] {7 * BS + 1, 9 * BS + 3}, new long[] {30 * BS + 1, 30 * BS + 3}), "below gate");
    }

    /** Above the gate with every block present, the pass is skipped by the coverage test. */
    @Test
    public void testInsertAboveGateIntoCoveredBlocks() {
        final RspBitmap rb = singletonsAtEveryBlock(0, OVER_GATE);
        SortedRanges sr = SortedRanges.makeSingleRange(10 * BS + 1, 10 * BS + 3);
        for (int i = 11; i < OVER_GATE; i += 3) {
            sr = sr.addRange(i * BS + 1, i * BS + 3);
            sr = sr.addRange(i * BS + 5, i * BS + 9);
        }
        checkInsert(rb, sr, "covered blocks above gate");
    }

    /** Above the gate with blocks missing, the pass runs and makes room for them. */
    @Test
    public void testInsertAboveGateIntoMissingBlocks() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < OVER_GATE; ++i) {
            rb = rb.appendUnsafe(2L * i * BS + 7); // even blocks only
        }
        rb.finishMutations();
        SortedRanges sr = SortedRanges.makeSingleRange(BS + 1, BS + 3);
        for (int i = 3; i < 2 * OVER_GATE; i += 4) {
            sr = sr.addRange(i * BS + 1, i * BS + 3); // odd blocks we lack
            sr = sr.addRange((i + 1) * BS + 100, (i + 1) * BS + 200); // even blocks we have
        }
        checkInsert(rb, sr, "missing blocks above gate");
    }

    /** The shifted insert makes the same decisions, on the shifted keys. */
    @Test
    public void testShiftedInsertAboveGate() {
        final RspBitmap receiver = singletonsAtEveryBlock(100, OVER_GATE);
        SortedRanges sr = SortedRanges.makeSingleRange(1, 3);
        for (int i = 1; i < 30; ++i) {
            sr = sr.addRange(i * BS + 1, i * BS + 3);
        }
        for (final long shift : new long[] {100 * BS, 100 * BS + 100, 50 * BS, 400 * BS + 5}) {
            final String expected =
                    render(unionRanges(RowSetTestCommon.rangesOf(receiver), shiftRanges(rangesOf(sr), shift)));
            final OrderedLongSet result = receiver.deepCopy().ixInsertWithShift(shift, sr);
            final List<long[]> got = new ArrayList<>();
            result.ixForEachLongRange((s, e) -> {
                got.add(new long[] {s, e});
                return true;
            });
            assertEquals("shift " + shift, expected, render(got));
            ((RspBitmap) result).validate("shift " + shift);
        }
    }

    /** A search that starts at the span holding the key returns it, and otherwise behaves as before. */
    @Test
    public void testSearchFromTheSpanHoldingTheKey() {
        final RspBitmap rb = singletonsAtEveryBlock(0, 20);
        assertEquals(5, rb.getSpanIndex(5, 5 * BS + 1));
        assertEquals(6, rb.getSpanIndex(5, 6 * BS + 1));
        assertEquals(19, rb.getSpanIndex(5, 19 * BS + 1));
        assertEquals("a key before the start is reported as not found there", -6, rb.getSpanIndex(5, 4 * BS + 1));
        assertEquals(-21, rb.getSpanIndex(5, 25 * BS));

        RspBitmap withFull = RspBitmap.makeEmpty();
        withFull = withFull.appendRangeUnsafe(0, 4 * BS - 1); // full block span over blocks 0..3
        withFull = withFull.appendUnsafe(4 * BS + 7);
        withFull.finishMutations();
        assertEquals("a key inside the starting full block span, not at its first block", 0,
                withFull.getSpanIndex(0, 2 * BS + 1));
        assertEquals(1, withFull.getSpanIndex(0, 4 * BS + 1));
    }
}
