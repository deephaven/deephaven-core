//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@code insertWithShift} of a {@link SortedRanges} into an {@link RspBitmap}. The shift is an arbitrary offset, not
 * necessarily a multiple of the block size, so the blocks the shifted ranges land in are not the blocks they came from.
 */
public class RspBitmapInsertWithShiftTest {

    private static final long BS = BLOCK_SIZE;

    /** Union of two ascending disjoint range lists, by interval arithmetic. Independent of the code under test. */
    private static List<long[]> unionRanges(final List<long[]> a, final List<long[]> b) {
        final List<long[]> all = new ArrayList<>(a.size() + b.size());
        all.addAll(a);
        all.addAll(b);
        all.sort(Comparator.comparingLong(r -> r[0]));
        final List<long[]> out = new ArrayList<>();
        for (final long[] r : all) {
            if (!out.isEmpty() && r[0] <= out.get(out.size() - 1)[1] + 1) {
                final long[] prev = out.get(out.size() - 1);
                prev[1] = Math.max(prev[1], r[1]);
            } else {
                out.add(new long[] {r[0], r[1]});
            }
        }
        return out;
    }

    private static List<long[]> shifted(final List<long[]> ranges, final long shift) {
        final List<long[]> out = new ArrayList<>(ranges.size());
        for (final long[] r : ranges) {
            out.add(new long[] {r[0] + shift, r[1] + shift});
        }
        return out;
    }

    private static void checkInsertWithShift(final RspBitmap receiver, final SortedRanges sr, final long shift) {
        final List<long[]> expected = unionRanges(rangesOf(receiver), shifted(rangesOf(sr.toRsp()), shift));
        final RspBitmap start = receiver.deepCopy();
        final Object result = start.ixInsertWithShift(shift, sr);
        assertTrue("result must be an RspBitmap for these shapes", result instanceof RspBitmap);
        final RspBitmap actual = (RspBitmap) result;
        actual.validate("after insertWithShift");
        assertEquals(render(expected), render(rangesOf(actual)));
        long card = 0;
        for (final long[] r : expected) {
            card += r[1] - r[0] + 1;
        }
        assertEquals("cardinality", card, actual.getCardinality());
    }

    private static RspBitmap containersAtEvenBlocks(final int blocks) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < blocks; ++i) {
            rb = rb.appendRangeUnsafe(2L * i * BS + 100, 2L * i * BS + 140);
        }
        rb.finishMutations();
        return rb;
    }

    private static SortedRanges srOf(final long[]... ranges) {
        SortedRanges sr = SortedRanges.makeSingleRange(ranges[0][0], ranges[0][1]);
        for (int i = 1; i < ranges.length; ++i) {
            sr = sr.addRange(ranges[i][0], ranges[i][1]);
            assertTrue("ranges must fit a SortedRanges", sr != null);
        }
        return sr;
    }

    @Test
    public void testBlockAlignedShiftIntoNewBlocks() {
        checkInsertWithShift(containersAtEvenBlocks(8),
                srOf(new long[] {5, 9}, new long[] {2 * BS + 5, 2 * BS + 9}), 1 * BS);
    }

    @Test
    public void testUnalignedShiftIntoNewBlocks() {
        // The shift is not a multiple of the block size, so each range lands in a different block than it came from.
        checkInsertWithShift(containersAtEvenBlocks(8),
                srOf(new long[] {5, 9}, new long[] {2 * BS + 5, 2 * BS + 9}), 1 * BS + 12345);
    }

    @Test
    public void testUnalignedShiftStraddlingABlockBoundary() {
        // After shifting, this range spans two blocks.
        checkInsertWithShift(containersAtEvenBlocks(6), srOf(new long[] {BS - 5, BS + 5}), 3 * BS + 3);
    }

    @Test
    public void testShiftIntoOccupiedBlocks() {
        checkInsertWithShift(containersAtEvenBlocks(6),
                srOf(new long[] {200, 240}, new long[] {2 * BS + 200, 2 * BS + 240}), 0);
    }

    @Test
    public void testShiftPastOurEnd() {
        checkInsertWithShift(containersAtEvenBlocks(4), srOf(new long[] {5, 9}), 40 * BS);
    }

    @Test
    public void testWholeBlockRangesAfterShift() {
        checkInsertWithShift(containersAtEvenBlocks(8), srOf(new long[] {0, 3 * BS - 1}), 5 * BS);
    }

    @Test
    public void testRandomShifts() {
        final Random rand = new Random(23407);
        for (int trial = 0; trial < 2000; ++trial) {
            final RspBitmap recv = containersAtEvenBlocks(3 + rand.nextInt(12));
            final List<long[]> argRanges = new ArrayList<>();
            long cursor = rand.nextInt(1000);
            final int count = 1 + rand.nextInt(10);
            for (int i = 0; i < count; ++i) {
                final long s = cursor + 2 + rand.nextInt((int) BS);
                final long e = s + (rand.nextInt(6) == 0 ? rand.nextInt(2 * (int) BS) : rand.nextInt(300));
                argRanges.add(new long[] {s, e});
                cursor = e + 1;
            }
            SortedRanges sr = SortedRanges.makeSingleRange(argRanges.get(0)[0], argRanges.get(0)[1]);
            boolean fits = true;
            for (int i = 1; i < argRanges.size() && fits; ++i) {
                final SortedRanges next = sr.addRange(argRanges.get(i)[0], argRanges.get(i)[1]);
                if (next == null) {
                    fits = false;
                } else {
                    sr = next;
                }
            }
            if (!fits) {
                continue;
            }
            // A mix of aligned, unaligned, and zero shifts.
            final long shift = switch (rand.nextInt(4)) {
                case 0 -> 0L;
                case 1 -> (long) (1 + rand.nextInt(20)) * BS;
                case 2 -> (long) rand.nextInt(3 * (int) BS);
                default -> (long) (1 + rand.nextInt(20)) * BS + rand.nextInt((int) BS);
            };
            checkInsertWithShift(recv, sr, shift);
        }
    }
}
