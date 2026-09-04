//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.RowSetUtils;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.container.BitmapContainer;
import io.deephaven.engine.rowset.impl.rsp.container.RunContainer;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Bulk operations that rank keys and positions within one RSP block cost time linear in the number of elements they
 * handle: {@code RspArray.getKeysForPositions}, {@code WritableRowSetImpl.subSetForPositions(RowSequence)},
 * {@link RowSetShiftData#apply(WritableRowSet)} through a {@link RowSequence.Iterator}, and
 * {@link RowSetUtils#forAllInvertedLongRanges}. Each ranks through a cursor that resumes from the previous element
 * rather than counting from the start of the block's container, where a bitmap holds 1024 words and a run container up
 * to thousands of runs.
 *
 * <p>
 * Each test times a single bulk call at P and 4P elements inside one block and takes the best of several runs; linear
 * cost predicts a ratio near 4, quadratic near 16. The bound leaves room for timing noise while staying well clear of
 * quadratic.
 */
@Category(OutOfBandTest.class)
public class RspRankCursorLinearityTest {

    private static final int REPS = 7;
    private static final double MAX_RATIO = 8.0;

    private static long minNanos(final Runnable r) {
        for (int i = 0; i < 3; ++i) {
            r.run(); // warm up
        }
        long best = Long.MAX_VALUE;
        for (int i = 0; i < REPS; ++i) {
            final long t0 = System.nanoTime();
            r.run();
            best = Math.min(best, System.nanoTime() - t0);
        }
        return best;
    }

    /** Keys 0, 2, 4, ..., 2*(keys-1): one block whose container is a BitmapContainer. */
    private static RowSet bitmapBlock(final int keys) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < keys; ++i) {
            b.appendKey(2L * i);
        }
        final RowSet rs = b.build();
        final OrderedLongSet inner = ((WritableRowSetImpl) rs).getInnerSet();
        assertTrue(inner instanceof RspBitmap);
        final RspBitmap rb = (RspBitmap) inner;
        assertEquals(1, rb.getSize());
        assertTrue("expected BitmapContainer, got " + rb.getSpans()[0].getClass(),
                rb.getSpans()[0] instanceof BitmapContainer);
        return rs;
    }

    /** {@code runs} runs of 16 keys, spaced 43 apart: one block whose container is a RunContainer. */
    private static RowSet runBlock(final int runs) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int r = 0; r < runs; ++r) {
            b.appendRange(43L * r, 43L * r + 15);
        }
        final RowSet rs = b.build();
        final OrderedLongSet inner = ((WritableRowSetImpl) rs).getInnerSet();
        assertTrue(inner instanceof RspBitmap);
        final RspBitmap rb = (RspBitmap) inner;
        assertEquals(1, rb.getSize());
        assertTrue("expected RunContainer, got " + rb.getSpans()[0].getClass(),
                rb.getSpans()[0] instanceof RunContainer);
        return rs;
    }

    private static PrimitiveIterator.OfLong positions(final long n) {
        return new PrimitiveIterator.OfLong() {
            long next = 0;

            @Override
            public long nextLong() {
                return next++;
            }

            @Override
            public boolean hasNext() {
                return next < n;
            }
        };
    }

    private static long timeGetKeysForPositions(final RowSet rs, final int p) {
        final long[] sink = new long[1];
        final LongConsumer sinkConsumer = v -> sink[0] += v;
        return minNanos(() -> rs.getKeysForPositions(positions(p), sinkConsumer));
    }

    /** Every other position in [0, 2p): p single-position ranges, so the contiguous fast path does not apply. */
    private static long timeSubSetForPositions(final RowSet rs, final int p) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < p; ++i) {
            b.appendKey(2L * i);
        }
        try (final RowSet positions = b.build()) {
            return minNanos(() -> {
                try (final WritableRowSet sub = rs.subSetForPositions(positions)) {
                    assertEquals(p, sub.size());
                }
            });
        }
    }

    /** {@code s} shift windows [8i, 8i+3] by +1, all inside the receiver's single block. */
    private static long timeShiftApply(final RowSet rs, final int s) {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        for (int i = 0; i < s; ++i) {
            b.shiftRange(8L * i, 8L * i + 3, 1);
        }
        final RowSetShiftData sd = b.build();
        return minNanos(() -> {
            try (final WritableRowSet copy = rs.copy()) {
                sd.apply(copy);
                assertEquals(rs.size(), copy.size());
            }
        });
    }

    /** Invert {@code d} single-key ranges (keys 8i, all present) against the receiver. */
    private static long timeForAllInvertedLongRanges(final RowSet rs, final int d) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < d; ++i) {
            b.appendKey(8L * i);
        }
        final long[] sink = new long[1];
        try (final RowSet dest = b.build()) {
            return minNanos(() -> RowSetUtils.forAllInvertedLongRanges(rs, dest, (s, e) -> sink[0] += e - s));
        }
    }

    private static void assertSubQuadratic(final String what, final long small, final long big) {
        final double ratio = big / (double) small;
        System.out.println(what + ": small=" + small / 1_000_000.0 + "ms, big=" + big / 1_000_000.0
                + "ms, ratio=" + ratio);
        assertTrue(what + " scaled super-linearly for 4x the elements in one bulk call: ratio=" + ratio,
                ratio < MAX_RATIO);
    }

    @Test
    public void testGetKeysForPositionsInBitmapBlock() {
        final RowSet rs = bitmapBlock(32768);
        final long small = timeGetKeysForPositions(rs, 8192);
        final long big = timeGetKeysForPositions(rs, 32768);
        assertSubQuadratic("bitmap getKeysForPositions", small, big);
    }

    @Test
    public void testGetKeysForPositionsInRunBlock() {
        final RowSet rs = runBlock(1500); // 24000 keys
        final long small = timeGetKeysForPositions(rs, 6000);
        final long big = timeGetKeysForPositions(rs, 24000);
        assertSubQuadratic("run getKeysForPositions", small, big);
    }

    @Test
    public void testSubSetForPositionsInBitmapBlock() {
        final RowSet rs = bitmapBlock(32768);
        final long small = timeSubSetForPositions(rs, 4096);
        final long big = timeSubSetForPositions(rs, 16384);
        assertSubQuadratic("bitmap subSetForPositions", small, big);
    }

    @Test
    public void testShiftDataApplyOnBitmapBlock() {
        final RowSet rs = bitmapBlock(32768);
        final long small = timeShiftApply(rs, 2048);
        final long big = timeShiftApply(rs, 8192);
        assertSubQuadratic("bitmap RowSetShiftData.apply single call", small, big);
    }

    @Test
    public void testForAllInvertedLongRangesOnBitmapBlock() {
        final RowSet rs = bitmapBlock(32768);
        final long small = timeForAllInvertedLongRanges(rs, 2048);
        final long big = timeForAllInvertedLongRanges(rs, 8192);
        assertSubQuadratic("bitmap forAllInvertedLongRanges", small, big);
    }
}
