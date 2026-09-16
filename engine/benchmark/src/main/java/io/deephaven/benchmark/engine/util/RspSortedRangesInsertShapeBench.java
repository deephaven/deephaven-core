//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

/**
 * Inserting one {@link SortedRanges} into an {@link RspBitmap}, across the shapes that decide whether the pre-pass in
 * {@code RspBitmap.insertOrderedLongSetUnsafeNoWriteCheck(SortedRanges)} pays for itself.
 *
 * <p>
 * The target holds {@code spans} singleton spans, one in every other block, so the blocks between them are free. The
 * incoming set holds {@code ranges} single keys spread evenly over the target's key range, {@code newSpanPercent} of
 * them in free blocks. A key in a free block makes the insert create a span, which without the pre-pass means shifting
 * the tail of the spans array; a key in an occupied block updates that span in place. With more ranges than spans the
 * ranges share blocks, several to a block, and the free-or-occupied choice is made per block.
 *
 * <p>
 * {@link #ixInsert} is the current path with the pre-pass; {@link #addRangesDirect} is the body that method had before
 * DH-23407. Both start from a fresh copy of the target, and {@link #copyOnly} measures that copy alone so it can be
 * subtracted.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1)
public class RspSortedRangesInsertShapeBench {

    /** Spans in the target's array before the insert. */
    @Param({"32", "128", "512", "2048", "8192"})
    private int spans;

    /** Single-key ranges in the incoming set. */
    @Param({"32", "1024"})
    private int ranges;

    /**
     * Percentage of the incoming ranges (or of the blocks they share) that fall in blocks the target has no span for.
     */
    @Param({"0", "10", "100"})
    private int newSpanPercent;

    private RspBitmap target;
    private SortedRanges incoming;

    @Setup
    public void setup() {
        target = new RspBitmap();
        for (int j = 0; j < spans; ++j) {
            target = target.add(occupiedBlockKey(j));
        }
        SortedRanges sr = SortedRanges.makeEmpty();
        int newSpans = 0;
        long lastBlock = -1;
        for (int r = 0; r < ranges; ++r) {
            // The block pair this range goes to, ascending in r; and the ordinal that decides free or occupied, which
            // is the range itself when every range has its own pair and the pair when they share it.
            final int pair = (int) ((long) r * spans / ranges);
            final int ordinal = ranges <= spans ? r : pair;
            final boolean free = (long) (ordinal + 1) * newSpanPercent / 100 > (long) ordinal * newSpanPercent / 100;
            final long blockKey = free ? occupiedBlockKey(pair) + RspBitmap.BLOCK_SIZE : occupiedBlockKey(pair);
            if (free && blockKey != lastBlock) {
                ++newSpans;
            }
            lastBlock = blockKey;
            // Low bits ascend with r, so keys sharing a block stay in order, and by two so they stay separate ranges;
            // starting at 1 keeps clear of the target's singleton.
            sr = sr.append(blockKey + 1 + 2 * (r % (RspBitmap.BLOCK_SIZE / 2 - 1)));
            if (sr == null) {
                throw new IllegalStateException("incoming set does not fit in a SortedRanges");
            }
        }
        incoming = sr;
        int rangeCount = 0;
        try (final io.deephaven.engine.rowset.RowSet.RangeIterator it = sr.getRangeIterator()) {
            while (it.hasNext()) {
                it.next();
                ++rangeCount;
            }
        }
        if (rangeCount != ranges) {
            throw new IllegalStateException("incoming set has " + rangeCount + " ranges, expected " + ranges);
        }
        System.out.println("spans=" + spans + " ranges=" + ranges + " newSpanPercent=" + newSpanPercent
                + " newSpans=" + newSpans + " incomingType=" + sr.getClass().getSimpleName());
    }

    private static long occupiedBlockKey(final int pair) {
        return 2L * pair * RspBitmap.BLOCK_SIZE;
    }

    // Neither insert finishes its mutations: rebuilding the cardinality cache costs the same on both paths and, being
    // linear in the span count, would swamp a small insert into a long array. Each method returns the bitmap it
    // mutated so that every update to it, including those inside existing spans, is observable.

    @Benchmark
    public RspBitmap copyOnly() {
        return target.deepCopy();
    }

    @Benchmark
    public RspBitmap prePassInsert() {
        final RspBitmap copy = target.deepCopy();
        copy.insertOrderedLongSetUnsafeNoWriteCheck(incoming);
        return copy;
    }

    @Benchmark
    public RspBitmap addRangesDirect() {
        final RspBitmap copy = target.deepCopy();
        copy.addRangesUnsafeNoWriteCheck(incoming.getRangeIterator());
        return copy;
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RspSortedRangesInsertShapeBench.class);
    }
}
