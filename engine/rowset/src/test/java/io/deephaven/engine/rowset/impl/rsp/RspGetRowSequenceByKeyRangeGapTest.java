//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * A key range may start in a block the rowset has no span for. Locating that key then lands on the span at the
 * insertion point for its block, and the search within that span has to report a position at the span's start rather
 * than treating the key as though it belonged to the span's own block.
 */
public class RspGetRowSequenceByKeyRangeGapTest {

    private static final long BS = BLOCK_SIZE;

    /** Built as an RspBitmap explicitly: a handful of ranges would otherwise be backed by SortedRanges. */
    private static WritableRowSet rspOf(final long[]... ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long[] r : ranges) {
            rb = rb.appendRangeUnsafe(r[0], r[1]);
        }
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    private static List<Long> keysOf(final RowSequence rowSequence) {
        final List<Long> out = new ArrayList<>();
        rowSequence.forAllRowKeys(out::add);
        return out;
    }

    private static List<Long> keysOf(final RowSet rs) {
        final List<Long> out = new ArrayList<>();
        rs.forAllRowKeys(out::add);
        return out;
    }

    /** getRowSequenceByKeyRange must agree with subSetByKeyRange, which takes a different path. */
    private static void check(final WritableRowSet rs, final long start, final long end) {
        try (final RowSequence seq = rs.getRowSequenceByKeyRange(start, end);
                final WritableRowSet expected = rs.subSetByKeyRange(start, end)) {
            assertEquals("keys for [" + start + ", " + end + "]", keysOf(expected), keysOf(seq));
            assertEquals("size for [" + start + ", " + end + "]", expected.size(), seq.size());
        }
    }

    /** The whole rowset lies in one late block, and the range starts far before it. */
    @Test
    public void testRangeStartingBeforeTheOnlySpan() {
        try (final WritableRowSet rs = rspOf(new long[] {2422280, 2422281})) {
            check(rs, BS - 2, 3181278);
        }
    }

    /** The range starts in a gap between two spans, so it must not drop the span that follows. */
    @Test
    public void testRangeStartingInAGapBetweenSpans() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 5}, new long[] {2 * BS, 2 * BS}, new long[] {2 * BS + 8,
                2 * BS + 8})) {
            check(rs, BS + 1, 2 * BS + 8);
        }
    }

    /** Gap starts against a container, a singleton and a full block span in turn. */
    @Test
    public void testGapStartAgainstEachSpanKind() {
        try (final WritableRowSet container = rspOf(new long[] {4 * BS + 10, 4 * BS + 40})) {
            check(container, BS, 5 * BS);
        }
        try (final WritableRowSet singleton = rspOf(new long[] {4 * BS + 10, 4 * BS + 10})) {
            check(singleton, BS, 5 * BS);
        }
        try (final WritableRowSet fullBlock = rspOf(new long[] {4 * BS, 5 * BS - 1})) {
            check(fullBlock, BS, 5 * BS);
        }
    }

    /**
     * The range ends in the gap before a span's first key, so the last key it includes belongs to the span before that
     * one. A full block span at block 5, a container at block 6, and an end key inside block 6 but below the
     * container's first key.
     */
    @Test
    public void testRangeEndingInAGapBeforeASpansFirstKey() {
        try (final WritableRowSet rs = rspOf(new long[] {5 * BS, 6 * BS - 1}, new long[] {6 * BS + 677,
                6 * BS + 729})) {
            check(rs, 5 * BS + 3364, 6 * BS + 95);
        }
    }

    /** The same shape where the range ends in a gap before a singleton span, and before a full block span. */
    @Test
    public void testRangeEndingInAGapBeforeOtherSpanKinds() {
        try (final WritableRowSet singleton = rspOf(new long[] {5 * BS, 6 * BS - 1}, new long[] {6 * BS + 700,
                6 * BS + 700})) {
            check(singleton, 5 * BS + 10, 6 * BS + 95);
        }
        try (final WritableRowSet fullBlock = rspOf(new long[] {5 * BS, 6 * BS - 1}, new long[] {8 * BS, 9 * BS - 1})) {
            check(fullBlock, 5 * BS + 10, 7 * BS + 95);
        }
    }

    /** Randomized: ranges whose ends land in gaps, in blocks before, inside and after the rowset. */
    @Test
    public void testRandomKeyRangesIncludingGaps() {
        final Random rand = new Random(23491);
        for (int trial = 0; trial < 500; ++trial) {
            final List<long[]> ranges = new ArrayList<>();
            long block = 1 + rand.nextInt(3);
            for (int i = 0; i < 6; ++i) {
                final long base = block * BS;
                if (rand.nextInt(4) == 0) {
                    ranges.add(new long[] {base, base + BS - 1}); // full block span
                } else if (rand.nextInt(3) == 0) {
                    ranges.add(new long[] {base + rand.nextInt(100), base + rand.nextInt(100)}); // maybe singleton
                } else {
                    final long s = base + rand.nextInt(1000);
                    ranges.add(new long[] {s, s + rand.nextInt(500)});
                }
                block += 1 + rand.nextInt(3); // leave gaps
            }
            final List<long[]> fixed = new ArrayList<>();
            for (final long[] r : ranges) {
                fixed.add(new long[] {Math.min(r[0], r[1]), Math.max(r[0], r[1])});
            }
            try (final WritableRowSet rs = rspOf(fixed.toArray(new long[0][]))) {
                for (int k = 0; k < 4; ++k) {
                    final long lo = (long) rand.nextInt((int) (block * BS));
                    final long hi = lo + rand.nextInt((int) (2 * BS));
                    check(rs, lo, hi);
                }
            }
        }
    }
}
