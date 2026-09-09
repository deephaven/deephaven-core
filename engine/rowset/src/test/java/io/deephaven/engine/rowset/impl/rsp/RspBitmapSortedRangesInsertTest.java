//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.deephaven.engine.rowset.impl.RowSetTestCommon;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.shiftRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesImplOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.unionRanges;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Inserting a {@link SortedRanges} into an {@link RspBitmap} first makes room, in one pass, for the blocks it only
 * partially covers and we do not have. These cover what that pass has to get right: several ranges landing in one
 * block, blocks a range covers completely, blocks already covered by a full block span of ours, and ranges past our
 * end.
 */
public class RspBitmapSortedRangesInsertTest {

    private static final long BS = BLOCK_SIZE;

    private static List<long[]> rangesOf(final SortedRanges sr) {
        final List<long[]> out = new ArrayList<>();
        sr.forEachLongRange((s, e) -> {
            out.add(new long[] {s, e});
            return true;
        });
        return out;
    }

    private static void checkInsert(final RspBitmap receiver, final SortedRanges sr) {
        // Compared as ranges: the fixtures hold whole blocks, far too many keys to enumerate.
        final String expected = render(unionRanges(RowSetTestCommon.rangesOf(receiver), rangesOf(sr)));
        final RspBitmap w = receiver.writeCheck();
        w.insertOrderedLongSetUnsafeNoWriteCheck(sr);
        w.finishMutations();
        w.validate("after insert");
        assertEquals(expected, render(RowSetTestCommon.rangesOf(w)));
    }

    private static RspBitmap containersAtEvenBlocks(final int blocks) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < blocks; ++i) {
            rb = rb.appendRangeUnsafe(2L * i * BS + 100, 2L * i * BS + 140);
        }
        rb.finishMutations();
        return rb;
    }

    /** Several ranges inside one block we lack: only one span may be created for it. */
    @Test
    public void testMultipleRangesInOneMissingBlock() {
        checkInsert(containersAtEvenBlocks(6),
                sortedRangesImplOf(new long[] {3 * BS + 5, 3 * BS + 10},
                        new long[] {3 * BS + 20, 3 * BS + 30},
                        new long[] {3 * BS + 40, 3 * BS + 50}));
    }

    /** A range covering whole blocks, with partial blocks at each end. */
    @Test
    public void testRangeSpanningWholeAndPartialBlocks() {
        checkInsert(containersAtEvenBlocks(8),
                sortedRangesImplOf(new long[] {3 * BS + 300, 6 * BS + 40}));
    }

    /** A range that covers its blocks exactly, so no partial-block span is needed at all. */
    @Test
    public void testRangeCoveringWholeBlocksExactly() {
        checkInsert(containersAtEvenBlocks(8),
                sortedRangesImplOf(new long[] {3 * BS, 5 * BS + BLOCK_LAST}));
    }

    /** Blocks already covered by a multi-block full block span of ours need no new span. */
    @Test
    public void testBlocksCoveredByOurFullBlockSpan() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0, 4L * BS - 1); // full block span over blocks 0..3
        rb = rb.appendRangeUnsafe(8L * BS + 100, 8L * BS + 140);
        rb.finishMutations();
        checkInsert(rb, sortedRangesImplOf(new long[] {1 * BS + 5, 1 * BS + 9},
                new long[] {2 * BS + 5, 2 * BS + 9},
                new long[] {6 * BS + 5, 6 * BS + 9}));
    }

    /** Ranges beyond our last block take the append path. */
    @Test
    public void testRangesPastOurEnd() {
        checkInsert(containersAtEvenBlocks(4),
                sortedRangesImplOf(new long[] {20 * BS + 5, 20 * BS + 9},
                        new long[] {22 * BS + 5, 22 * BS + 9}));
    }

    /** A first range before our start, so the new span goes at index 0. */
    @Test
    public void testRangeBeforeOurStart() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(10L * BS + 100, 10L * BS + 140);
        rb = rb.appendRangeUnsafe(12L * BS + 100, 12L * BS + 140);
        rb.finishMutations();
        checkInsert(rb, sortedRangesImplOf(new long[] {2 * BS + 5, 2 * BS + 9}));
    }

    /**
     * A range whose first block is still within our spans but whose last block is past them, followed by ranges wholly
     * past them. The pass has to make room for that first block before it stops.
     */
    @Test
    public void testRangeStraddlingOurEnd() {
        final RspBitmap rb = containersAtEvenBlocks(4); // blocks 0,2,4,6
        checkInsert(rb, sortedRangesImplOf(new long[] {5 * BS + 300, 9 * BS + 40},
                new long[] {12 * BS + 5, 12 * BS + 9},
                new long[] {14 * BS + 5, 14 * BS + 9}));
    }

    /** A partially covered block we lack, sitting exactly at our last block. */
    @Test
    public void testPartialBlockAtOurLastBlock() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0 * BS + 100, 0 * BS + 140);
        rb = rb.appendRangeUnsafe(4L * BS + 100, 4L * BS + 140);
        rb.finishMutations();
        // Block 4 is our last; block 3 is missing and only partially covered.
        checkInsert(rb, sortedRangesImplOf(new long[] {3 * BS + 5, 3 * BS + 9},
                new long[] {4 * BS + 200, 4 * BS + 240}));
    }

    /** A range covering exactly one whole block we lack: its own full block span, and no stepping back a block. */
    @Test
    public void testSingleBlockWhollyCoveredRange() {
        checkInsert(containersAtEvenBlocks(6),
                sortedRangesImplOf(new long[] {3 * BS, 3 * BS + BLOCK_LAST}));
    }

    /** Single-block ranges that stop short of the block end: the run computation must find no run at all. */
    @Test
    public void testSingleBlockPartialRangesInBlockZero() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(4L * BS + 100, 4L * BS + 140);
        rb.finishMutations();
        // Block 0 is missing and these cover only part of it, including a range starting at key 0.
        checkInsert(rb, sortedRangesImplOf(new long[] {0, 10}, new long[] {20, 30}));
    }

    /** A run of complete blocks that covers spans of ours, so it has to be absorbed rather than placed as-is. */
    @Test
    public void testRunCoveringOurSpansNeedsAbsorption() {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int block = 0; block < 8; ++block) {
            rb = rb.appendRangeUnsafe(block * BS + 100, block * BS + 140);
        }
        rb.finishMutations();
        // Blocks 2..5 are covered completely and we have a container in each of them.
        checkInsert(rb, sortedRangesImplOf(new long[] {1 * BS + 500, 6 * BS + 40}));
    }

    /** A run immediately after a full block span of ours, which would have to merge with it. */
    @Test
    public void testRunAdjacentToOurFullBlockSpan() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0, 2L * BS - 1); // full block span over blocks 0 and 1
        rb = rb.appendRangeUnsafe(9L * BS + 100, 9L * BS + 140);
        rb.finishMutations();
        // Covers blocks 2 and 3 completely, directly adjacent to our full block span.
        checkInsert(rb, sortedRangesImplOf(new long[] {2 * BS, 4 * BS - 1}));
    }

    /**
     * A run of complete blocks whose first block we lack but whose later blocks we have: the run cannot be placed as-is
     * at the position found for its first block, because our spans for the later blocks are sitting there.
     */
    @Test
    public void testRunStartingInAMissingBlockOverOurLaterSpans() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(0 * BS + 100, 0 * BS + 140);
        rb = rb.appendRangeUnsafe(3L * BS + 100, 3L * BS + 140); // inside the run below
        rb = rb.appendRangeUnsafe(4L * BS + 100, 4L * BS + 140); // inside the run below
        rb = rb.appendRangeUnsafe(9L * BS + 100, 9L * BS + 140);
        rb.finishMutations();
        // Covers blocks 2,3,4 completely; we lack block 2 but hold 3 and 4.
        checkInsert(rb, sortedRangesImplOf(new long[] {2 * BS, 5 * BS - 1}));
    }

    @Test
    public void testRandom() {
        final Random rand = new Random(20260824);
        for (int trial = 0; trial < 100; ++trial) {
            RspBitmap rb = RspBitmap.makeEmpty();
            for (int block = 0; block < 30; ++block) {
                if (rand.nextInt(3) == 0) {
                    continue;
                }
                final long base = block * BS;
                if (rand.nextInt(6) == 0) {
                    final int len = 1 + rand.nextInt(3);
                    rb = rb.appendRangeUnsafe(base, base + len * BS - 1);
                    block += len - 1;
                } else {
                    final long start = base + rand.nextInt(1000);
                    rb = rb.appendRangeUnsafe(start, start + rand.nextInt(200));
                }
            }
            if (rb.isEmpty()) {
                continue;
            }
            rb.finishMutations();

            // A handful of ascending, disjoint, non-adjacent ranges.
            final List<long[]> ranges = new ArrayList<>();
            long cursor = rand.nextInt((int) BS);
            for (int r = 0; r < 8; ++r) {
                final long start = cursor + 2 + rand.nextInt((int) BS);
                final long end = start + (rand.nextInt(4) == 0 ? rand.nextInt(3 * (int) BS) : rand.nextInt(500));
                ranges.add(new long[] {start, end});
                cursor = end + 1;
            }
            SortedRanges sr = SortedRanges.makeSingleRange(ranges.get(0)[0], ranges.get(0)[1]);
            boolean fits = true;
            for (int i = 1; i < ranges.size() && fits; ++i) {
                final SortedRanges next = sr.addRange(ranges.get(i)[0], ranges.get(i)[1]);
                if (next == null) {
                    fits = false;
                } else {
                    sr = next;
                }
            }
            if (!fits) {
                continue;
            }
            checkInsert(rb, sr);
        }
    }

    /** A container in every one of the first {@code blocks} blocks. */
    private static RspBitmap containersAtEveryBlock(final int blocks) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < blocks; ++i) {
            rb = rb.appendRangeUnsafe(i * BS + 100, i * BS + 140);
        }
        rb.finishMutations();
        return rb;
    }

    /**
     * Ranges covering blocks 3j and 3j+1 completely, each absorbing two spans of ours; block 3j+2 is left alone so the
     * ranges do not coalesce. The spans absorbed are only compacted out once, after every range is in.
     */
    @Test
    public void testRangesAbsorbingTwoOfOurSpansEach() {
        final RspBitmap receiver = containersAtEveryBlock(40);
        SortedRanges sr = SortedRanges.makeSingleRange(0, 2 * BS - 1);
        for (int j = 1; j < 12; ++j) {
            sr = sr.addRange(3L * j * BS, (3L * j + 2) * BS - 1);
        }
        checkInsert(receiver, sr);
    }

    /** Ranges filling the one block between two full block spans of ours, so three spans become one. */
    @Test
    public void testRangesBridgingOurFullBlockSpans() {
        RspBitmap receiver = RspBitmap.makeEmpty();
        for (int i = 0; i < 40; ++i) {
            if (i % 3 == 1) {
                receiver = receiver.appendRangeUnsafe(i * BS + 100, i * BS + 140); // the block to be filled
            } else {
                receiver = receiver.appendRangeUnsafe(i * BS, (i + 1) * BS - 1); // full block span
            }
        }
        receiver.finishMutations();
        SortedRanges sr = SortedRanges.makeSingleRange(BS, 2 * BS - 1);
        for (int j = 1; j < 13; ++j) {
            sr = sr.addRange((3L * j + 1) * BS, (3L * j + 2) * BS - 1);
        }
        checkInsert(receiver, sr);
    }

    /** Partial blocks on either side of a run of whole blocks that absorb spans of ours, several times over. */
    @Test
    public void testRangesWithPartialEndsAroundAbsorbedBlocks() {
        final RspBitmap receiver = containersAtEveryBlock(60);
        SortedRanges sr = SortedRanges.makeSingleRange(BS - 10, 4 * BS + 10);
        for (int j = 1; j < 10; ++j) {
            final long base = 6L * j * BS;
            sr = sr.addRange(base + BS - 10, base + 4 * BS + 10);
        }
        checkInsert(receiver, sr);
    }

    /** The shifted insert takes the same batched path; the shift is not a whole number of blocks. */
    @Test
    public void testShiftedRangesAbsorbingOurSpans() {
        final RspBitmap receiver = containersAtEveryBlock(40);
        SortedRanges sr = SortedRanges.makeSingleRange(0, 2 * BS - 1);
        for (int j = 1; j < 12; ++j) {
            sr = sr.addRange(3L * j * BS, (3L * j + 2) * BS - 1);
        }
        for (final long shift : new long[] {0, BS, 100, BS + 100}) {
            final String expected =
                    render(unionRanges(RowSetTestCommon.rangesOf(receiver), shiftRanges(rangesOf(sr), shift)));
            final RspBitmap w = receiver.deepCopy();
            final OrderedLongSet result = w.ixInsertWithShift(shift, sr);
            final List<long[]> got = new ArrayList<>();
            result.ixForEachLongRange((s, e) -> {
                got.add(new long[] {s, e});
                return true;
            });
            assertEquals("shift " + shift, expected, render(got));
            if (result instanceof RspBitmap) {
                ((RspBitmap) result).validate("shift " + shift);
            }
        }
    }
}
