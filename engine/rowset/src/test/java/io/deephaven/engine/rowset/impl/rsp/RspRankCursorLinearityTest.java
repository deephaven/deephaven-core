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
 * Each test compares one bulk call at P elements with one at 4P elements inside one block; linear cost predicts a ratio
 * near 4, quadratic near 16, and the bound sits between them. The timing is arranged to survive a shared, busy machine:
 * each timed block repeats its call for at least {@link #BLOCK_NANOS}, so one scheduler slice cannot swallow it, and
 * the small and big blocks alternate for {@link #ROUNDS} rounds with each size keeping its best per-call time, so a
 * burst of contention lands on both sizes rather than on whichever ran second.
 */
@Category(OutOfBandTest.class)
public class RspRankCursorLinearityTest {

    private static final long BLOCK_NANOS = 100_000_000L;
    private static final int ROUNDS = 5;
    private static final double MAX_RATIO = 8.0;

    /**
     * Runs {@code r} back to back at least {@code minCalls} times and until at least {@link #BLOCK_NANOS} have elapsed,
     * whichever takes longer, and returns the average nanoseconds per call. The time bound keeps a block from ending
     * early when the calibration that produced {@code minCalls} ran slowly; the call floor keeps a block from doing
     * less work than the calibration did when it runs faster.
     */
    private static double timedBlock(final Runnable r, final int minCalls) {
        final long t0 = System.nanoTime();
        long calls = 0;
        long elapsed;
        do {
            r.run();
            ++calls;
            elapsed = System.nanoTime() - t0;
        } while (calls < minCalls || elapsed < BLOCK_NANOS);
        return elapsed / (double) calls;
    }

    /** Warms {@code r} up for about a block's worth of time and returns how many calls filled it. */
    private static int callsPerBlock(final Runnable r) {
        final long t0 = System.nanoTime();
        long calls = 0;
        do {
            r.run();
            ++calls;
        } while (System.nanoTime() - t0 < BLOCK_NANOS);
        return (int) Math.max(1, calls);
    }

    /** The best per-call time of each of {@code small} and {@code big} over alternating timed blocks. */
    private static double[] bestPerCall(final Runnable small, final Runnable big) {
        final int smallCalls = callsPerBlock(small);
        final int bigCalls = callsPerBlock(big);
        double bestSmall = Double.MAX_VALUE;
        double bestBig = Double.MAX_VALUE;
        for (int round = 0; round < ROUNDS; ++round) {
            bestSmall = Math.min(bestSmall, timedBlock(small, smallCalls));
            bestBig = Math.min(bestBig, timedBlock(big, bigCalls));
        }
        return new double[] {bestSmall, bestBig};
    }

    private static void assertLinear(final String what, final Runnable small, final Runnable big) {
        final double[] best = bestPerCall(small, big);
        final double ratio = best[1] / best[0];
        System.out.println(what + ": small=" + best[0] / 1_000_000.0 + "ms, big=" + best[1] / 1_000_000.0
                + "ms, ratio=" + ratio);
        assertTrue(what + " scaled super-linearly for 4x the elements in one bulk call: ratio=" + ratio,
                ratio < MAX_RATIO);
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

    /** Keys {@code step * i} for {@code i < count}. */
    private static RowSet everyNth(final int step, final int count) {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        for (int i = 0; i < count; ++i) {
            b.appendKey((long) step * i);
        }
        return b.build();
    }

    private static Runnable getKeysForPositions(final RowSet rs, final int p) {
        final long[] sink = new long[1];
        final LongConsumer sinkConsumer = v -> sink[0] += v;
        return () -> rs.getKeysForPositions(positions(p), sinkConsumer);
    }

    /** {@code positions} holds single positions with gaps between them, so the contiguous fast path does not apply. */
    private static Runnable subSetForPositions(final RowSet rs, final RowSet positions) {
        return () -> {
            try (final WritableRowSet sub = rs.subSetForPositions(positions)) {
                assertEquals(positions.size(), sub.size());
            }
        };
    }

    /** {@code s} shift windows [4i, 4i+1] by +1, all inside the receiver's single block. */
    private static RowSetShiftData shiftsOfTwo(final int s) {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        for (int i = 0; i < s; ++i) {
            b.shiftRange(4L * i, 4L * i + 1, 1);
        }
        return b.build();
    }

    private static Runnable shiftApply(final RowSet rs, final RowSetShiftData sd) {
        return () -> {
            try (final WritableRowSet copy = rs.copy()) {
                sd.apply(copy);
                assertEquals(rs.size(), copy.size());
            }
        };
    }

    /** Inverts the single-key ranges of {@code dest}, all of which the receiver holds, against the receiver. */
    private static Runnable invertedRanges(final RowSet rs, final RowSet dest) {
        final long[] sink = new long[1];
        return () -> RowSetUtils.forAllInvertedLongRanges(rs, dest, (s, e) -> sink[0] += e - s);
    }

    @Test
    public void testGetKeysForPositionsInBitmapBlock() {
        try (final RowSet rs = bitmapBlock(32768)) {
            assertLinear("bitmap getKeysForPositions", getKeysForPositions(rs, 8192), getKeysForPositions(rs, 32768));
        }
    }

    @Test
    public void testGetKeysForPositionsInRunBlock() {
        try (final RowSet rs = runBlock(1500)) { // 24000 keys
            assertLinear("run getKeysForPositions", getKeysForPositions(rs, 6000), getKeysForPositions(rs, 24000));
        }
    }

    @Test
    public void testSubSetForPositionsInBitmapBlock() {
        try (final RowSet rs = bitmapBlock(32768);
                final RowSet small = everyNth(2, 4096);
                final RowSet big = everyNth(2, 16384)) {
            assertLinear("bitmap subSetForPositions", subSetForPositions(rs, small), subSetForPositions(rs, big));
        }
    }

    @Test
    public void testShiftDataApplyOnBitmapBlock() {
        try (final RowSet rs = bitmapBlock(32768)) {
            assertLinear("bitmap RowSetShiftData.apply single call", shiftApply(rs, shiftsOfTwo(4096)),
                    shiftApply(rs, shiftsOfTwo(16384)));
        }
    }

    @Test
    public void testForAllInvertedLongRangesOnBitmapBlock() {
        try (final RowSet rs = bitmapBlock(32768);
                final RowSet small = everyNth(4, 4096);
                final RowSet big = everyNth(4, 16384)) {
            assertLinear("bitmap forAllInvertedLongRanges", invertedRanges(rs, small), invertedRanges(rs, big));
        }
    }
}
