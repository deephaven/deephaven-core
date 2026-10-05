//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseablePair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowSetShiftData#apply(long)} bisects the shift ranges rather than scanning them, and the row-set-consuming
 * methods ({@link RowSetShiftData#apply(WritableRowSet)}, {@link RowSetShiftData#unapply(WritableRowSet)},
 * {@link RowSetShiftData#unapply(WritableRowSet, long)},
 * {@link RowSetShiftData#extractParallelShiftedRowsFromPostShiftRowSet(RowSet)} and
 * {@link RowSetShiftData#forAllInRowSet(RowSet, RowSetShiftData.SingleElementShiftCallback)}) visit only the shift
 * ranges that reach the row set. These check them against straightforward per-key scans, including for keys landing in
 * the gaps between ranges and outside them entirely.
 */
public class RowSetShiftDataApplyTest {

    /** The scan {@code apply} replaced, kept here as the reference answer. */
    private static long applyByScan(final RowSetShiftData sd, final long keyToShift) {
        for (int idx = 0; idx < sd.size(); ++idx) {
            if (sd.getBeginRange(idx) > keyToShift) {
                return keyToShift;
            }
            if (sd.getEndRange(idx) >= keyToShift) {
                return keyToShift + sd.getShiftDelta(idx);
            }
        }
        return keyToShift;
    }

    private static void assertMatchesScanForKeysAround(final RowSetShiftData sd, final long maxKey) {
        for (long key = 0; key <= maxKey; ++key) {
            assertEquals("key=" + key, applyByScan(sd, key), sd.apply(key));
        }
    }

    @Test
    public void testEmpty() {
        assertMatchesScanForKeysAround(RowSetShiftData.EMPTY, 20);
    }

    @Test
    public void testSingleRange() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(10, 20, 5);
        final RowSetShiftData sd = b.build();
        // Below, inside, and above the only range.
        assertEquals(9, sd.apply(9));
        assertEquals(15, sd.apply(10));
        assertEquals(25, sd.apply(20));
        assertEquals(21, sd.apply(21));
        assertMatchesScanForKeysAround(sd, 40);
    }

    @Test
    public void testGapsBetweenRanges() {
        final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
        b.shiftRange(10, 20, -5);
        b.shiftRange(40, 50, 7);
        b.shiftRange(80, 80, 3);
        final RowSetShiftData sd = b.build();
        // Keys in the gaps are already in post-shift space.
        assertEquals(30, sd.apply(30));
        assertEquals(60, sd.apply(60));
        assertEquals(83, sd.apply(80));
        assertMatchesScanForKeysAround(sd, 100);
    }

    @Test
    public void testRandomAgainstScan() {
        final Random rand = new Random(42);
        for (int trial = 0; trial < 200; ++trial) {
            // One sign per trial, with gaps wider than any delta, so the shifted ranges stay ordered and disjoint --
            // which is what RowSetShiftData requires of its input.
            final int sign = (trial % 2 == 0) ? 1 : -1;
            final RowSetShiftData.Builder b = new RowSetShiftData.Builder();
            long next = 20 + rand.nextInt(10);
            final int ranges = 1 + rand.nextInt(12);
            for (int r = 0; r < ranges; ++r) {
                final long begin = next + 10 + rand.nextInt(10);
                final long end = begin + rand.nextInt(5);
                b.shiftRange(begin, end, sign * (1 + rand.nextInt(5)));
                next = end;
            }
            assertMatchesScanForKeysAround(b.build(), next + 20);
        }
    }

    /** The pre-shift key that a post-shift key came from, by a straightforward scan of the post-shift windows. */
    private static long unapplyByScan(final RowSetShiftData sd, final long postShiftKey, final long offset) {
        for (int idx = 0; idx < sd.size(); ++idx) {
            final long delta = sd.getShiftDelta(idx);
            if (sd.getBeginRange(idx) + offset + delta <= postShiftKey
                    && postShiftKey <= sd.getEndRange(idx) + offset + delta) {
                return postShiftKey - delta;
            }
        }
        return postShiftKey;
    }

    /** Whether a post-shift key lies in any post-shift window. */
    private static boolean inPostShiftWindow(final RowSetShiftData sd, final long postShiftKey) {
        return unapplyByScan(sd, postShiftKey, 0) != postShiftKey;
    }

    /** The (key, delta) callbacks forAllInRowSet makes: positive deltas descending, then negative deltas ascending. */
    private static List<long[]> forAllInRowSetByScan(final RowSetShiftData sd, final RowSet rowSet) {
        final List<long[]> calls = new ArrayList<>();
        for (int idx = sd.size() - 1; idx >= 0; --idx) {
            final long delta = sd.getShiftDelta(idx);
            if (delta < 0) {
                continue;
            }
            try (final RowSet.SearchIterator it = rowSet.reverseIterator()) {
                while (it.hasNext()) {
                    final long key = it.nextLong();
                    if (key >= sd.getBeginRange(idx) && key <= sd.getEndRange(idx)) {
                        calls.add(new long[] {key, delta});
                    }
                }
            }
        }
        for (int idx = 0; idx < sd.size(); ++idx) {
            final long delta = sd.getShiftDelta(idx);
            if (delta > 0) {
                continue;
            }
            final long begin = sd.getBeginRange(idx);
            final long end = sd.getEndRange(idx);
            rowSet.forAllRowKeys(key -> {
                if (key >= begin && key <= end) {
                    calls.add(new long[] {key, delta});
                }
            });
        }
        return calls;
    }

    private static void assertCallsEqual(final List<long[]> expected, final List<long[]> actual) {
        assertEquals("number of callbacks", expected.size(), actual.size());
        for (int ii = 0; ii < expected.size(); ++ii) {
            assertEquals("key of callback " + ii, expected.get(ii)[0], actual.get(ii)[0]);
            assertEquals("delta of callback " + ii, expected.get(ii)[1], actual.get(ii)[1]);
        }
    }

    /**
     * Checks every row-set-consuming method of {@code sd} against the per-key scans, treating {@code keys} as a
     * pre-shift row set for apply and forAllInRowSet, and as a post-shift row set for unapply and the parallel
     * extraction.
     */
    private static void assertRowSetMethodsMatchScan(final RowSetShiftData sd, final long... keys) {
        final RowSetBuilderRandom expectedApply = RowSetFactory.builderRandom();
        final RowSetBuilderRandom expectedUnapply = RowSetFactory.builderRandom();
        final RowSetBuilderRandom expectedUnapplyOffset = RowSetFactory.builderRandom();
        final RowSetBuilderRandom expectedPostShift = RowSetFactory.builderRandom();
        final RowSetBuilderRandom expectedPreShift = RowSetFactory.builderRandom();
        final long offset = 3;
        for (final long key : keys) {
            expectedApply.addKey(applyByScan(sd, key));
            expectedUnapply.addKey(unapplyByScan(sd, key, 0));
            expectedUnapplyOffset.addKey(unapplyByScan(sd, key, offset));
            if (inPostShiftWindow(sd, key)) {
                expectedPostShift.addKey(key);
                expectedPreShift.addKey(unapplyByScan(sd, key, 0));
            }
        }
        final String context = sd + " keys " + RowSetFactory.fromKeys(keys);
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(keys));
                final RowSet expected = expectedApply.build()) {
            assertEquals("apply " + context, expected, applied);
        }
        try (final WritableRowSet unapplied = sd.unapply(RowSetFactory.fromKeys(keys));
                final RowSet expected = expectedUnapply.build()) {
            assertEquals("unapply " + context, expected, unapplied);
        }
        try (final WritableRowSet unapplied = sd.unapply(RowSetFactory.fromKeys(keys), offset);
                final RowSet expected = expectedUnapplyOffset.build()) {
            assertEquals("unapply with offset " + context, expected, unapplied);
        }
        try (final RowSet postShift = RowSetFactory.fromKeys(keys);
                final SafeCloseablePair<RowSet, RowSet> extracted =
                        sd.extractParallelShiftedRowsFromPostShiftRowSet(postShift);
                final RowSet expectedPre = expectedPreShift.build();
                final RowSet expectedPost = expectedPostShift.build()) {
            assertEquals("extract pre-shift " + context, expectedPre, extracted.first);
            assertEquals("extract post-shift " + context, expectedPost, extracted.second);
        }
        try (final RowSet rowSet = RowSetFactory.fromKeys(keys)) {
            final List<long[]> calls = new ArrayList<>();
            sd.forAllInRowSet(rowSet, (key, delta) -> calls.add(new long[] {key, delta}));
            assertCallsEqual(forAllInRowSetByScan(sd, rowSet), calls);
        }
    }

    /** Ranges [100, 109] +5, [200, 209] -7, [300, 309] +6, [400, 409] -8. */
    private static RowSetShiftData mixedShifts() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 109, 5);
        builder.shiftRange(200, 209, -7);
        builder.shiftRange(300, 309, 6);
        builder.shiftRange(400, 409, -8);
        return builder.build();
    }

    @Test
    public void testRowSetEntirelyBeforeRanges() {
        final RowSetShiftData sd = mixedShifts();
        assertRowSetMethodsMatchScan(sd, 0, 5, 50, 90);
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(0, 5, 50, 90))) {
            assertEquals(RowSetFactory.fromKeys(0, 5, 50, 90), applied);
        }
    }

    @Test
    public void testRowSetEntirelyAfterRanges() {
        final RowSetShiftData sd = mixedShifts();
        assertRowSetMethodsMatchScan(sd, 500, 501, 1000);
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(500, 501, 1000))) {
            assertEquals(RowSetFactory.fromKeys(500, 501, 1000), applied);
        }
    }

    @Test
    public void testRowSetBetweenRanges() {
        final RowSetShiftData sd = mixedShifts();
        assertRowSetMethodsMatchScan(sd, 150, 160, 250);
        assertRowSetMethodsMatchScan(sd, 110, 199);
        assertRowSetMethodsMatchScan(sd, 310, 350, 399);
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(150, 160, 250))) {
            assertEquals(RowSetFactory.fromKeys(150, 160, 250), applied);
        }
    }

    @Test
    public void testRowSetOverlappingFirstRange() {
        final RowSetShiftData sd = mixedShifts();
        assertRowSetMethodsMatchScan(sd, 95, 100, 105, 109, 120);
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(95, 100, 105, 109, 120))) {
            assertEquals(RowSetFactory.fromKeys(95, 105, 110, 114, 120), applied);
        }
    }

    @Test
    public void testRowSetOverlappingLastRange() {
        final RowSetShiftData sd = mixedShifts();
        assertRowSetMethodsMatchScan(sd, 395, 400, 409, 420);
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(395, 400, 409, 420))) {
            assertEquals(RowSetFactory.fromKeys(392, 395, 401, 420), applied);
        }
    }

    @Test
    public void testRowSetSpanningAllRanges() {
        final RowSetShiftData sd = mixedShifts();
        assertRowSetMethodsMatchScan(sd, 0, 100, 150, 205, 300, 309, 405, 1000);
    }

    @Test
    public void testSmallRowSetAmongManyRanges() {
        // Ranges [10 * ii + 3, 10 * ii + 5] alternating +2 and -2, so the shifted windows stay ordered and disjoint.
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        final int numRanges = 10_000;
        for (int ii = 0; ii < numRanges; ++ii) {
            builder.shiftRange(10L * ii + 3, 10L * ii + 5, ii % 2 == 0 ? 2 : -2);
        }
        final RowSetShiftData sd = builder.build();
        for (final int middle : new int[] {0, 1, 4_999, 5_000, numRanges - 2, numRanges - 1}) {
            final long base = 10L * middle;
            assertRowSetMethodsMatchScan(sd, base + 4);
            assertRowSetMethodsMatchScan(sd, base + 1, base + 3, base + 5, base + 8);
            assertRowSetMethodsMatchScan(sd, base, base + 9);
            assertRowSetMethodsMatchScan(sd, base + 6, base + 7);
            // Two clusters with many ranges between them.
            assertRowSetMethodsMatchScan(sd, base + 4, (base + 50_000) % (10L * numRanges) + 4);
        }
        try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(10L * 5_000 + 4, 10L * 5_001 + 3))) {
            assertEquals(RowSetFactory.fromKeys(10L * 5_000 + 6, 10L * 5_001 + 1), applied);
        }
    }

    @Test
    public void testEmptyRowSetAndEmptyShifts() {
        assertRowSetMethodsMatchScan(mixedShifts());
        assertRowSetMethodsMatchScan(RowSetShiftData.EMPTY);
        assertRowSetMethodsMatchScan(RowSetShiftData.EMPTY, 0, 5, 100);
    }

    @Test
    public void testRandomRowSetsAgainstScan() {
        final Random rand = new Random(0x3341);
        for (int trial = 0; trial < 500; ++trial) {
            // Gaps between ranges are wider than twice any delta, so the shifted windows stay ordered and disjoint
            // whatever the mix of signs; the first range begins past the largest delta, so none shifts below zero.
            final int maxDelta = 1 + rand.nextInt(6);
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            long next = maxDelta + rand.nextInt(5);
            final int numRanges = rand.nextInt(40);
            final boolean mixed = rand.nextBoolean();
            final int sign = rand.nextBoolean() ? 1 : -1;
            for (int ii = 0; ii < numRanges; ++ii) {
                final long begin = next + 2L * maxDelta + 1 + rand.nextInt(10);
                final long end = begin + rand.nextInt(8);
                final int deltaSign = mixed ? (rand.nextBoolean() ? 1 : -1) : sign;
                builder.shiftRange(begin, end, deltaSign * (1 + rand.nextInt(maxDelta)));
                next = end;
            }
            final RowSetShiftData sd = builder.build();
            final long keySpace = next + 2L * maxDelta + 20;

            // Either a dense row set across the key space, or a tiny cluster anywhere in it.
            final long[] keys;
            if (rand.nextBoolean()) {
                keys = rand.longs(rand.nextInt(60), 0, keySpace).sorted().distinct().toArray();
            } else {
                final long clusterStart = (long) (rand.nextDouble() * keySpace);
                keys = rand.longs(1 + rand.nextInt(4), clusterStart, clusterStart + 1 + rand.nextInt(15))
                        .sorted().distinct().toArray();
            }
            assertRowSetMethodsMatchScan(sd, keys);
        }
    }

    /**
     * Applies one shift list of a million ranges to ten thousand tiny row sets near its end, and compares the total
     * time with that of a single apply to a row set holding a key in every range. Each tiny row set costs a gallop of
     * about forty probes into the shift list plus a few small row set operations, so the ten thousand applies together
     * take around a hundredth of one full walk; a walk from the start of the shift list would cost each tiny row set
     * nearly a full walk of its own, thousands of full walks in total. The bound of two full walks leaves a margin of
     * two orders of magnitude on either side against timing noise. Each measurement is the fastest of four repetitions,
     * the first of which also warms up both paths.
     */
    @Test
    public void testTinyRowSetsNearEndOfManyRangesAreBounded() {
        final int numRanges = 1_000_000;
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        for (int ii = 0; ii < numRanges; ++ii) {
            builder.shiftRange(10L * ii + 3, 10L * ii + 5, ii % 2 == 0 ? 2 : -2);
        }
        final RowSetShiftData sd = builder.build();

        final RowSetBuilderSequential everyRange = RowSetFactory.builderSequential();
        for (int ii = 0; ii < numRanges; ++ii) {
            everyRange.appendKey(10L * ii + 4);
        }
        try (final RowSet fullRowSet = everyRange.build()) {
            final int numTiny = 10_000;
            final long firstTinyRange = numRanges - numTiny;

            long fullNanos = Long.MAX_VALUE;
            long tinyNanos = Long.MAX_VALUE;
            for (int rep = 0; rep < 4; ++rep) {
                final long fullStart = System.nanoTime();
                try (final WritableRowSet applied = sd.apply(fullRowSet.copy())) {
                    assertEquals(fullRowSet.size(), applied.size());
                }
                fullNanos = Math.min(fullNanos, System.nanoTime() - fullStart);

                long shiftedSum = 0;
                final long tinyStart = System.nanoTime();
                for (int ii = 0; ii < numTiny; ++ii) {
                    final long base = 10L * (firstTinyRange + ii);
                    try (final WritableRowSet applied = sd.apply(RowSetFactory.fromKeys(base + 1, base + 4))) {
                        shiftedSum += applied.lastRowKey() - base;
                    }
                }
                tinyNanos = Math.min(tinyNanos, System.nanoTime() - tinyStart);
                // Even ranges move +2 and odd ranges -2, and numTiny is even, so the moved keys average out to 4.
                assertEquals(4L * numTiny, shiftedSum);
            }
            assertTrue("tiny applies took " + tinyNanos + "ns, a full walk " + fullNanos + "ns",
                    tinyNanos < 2 * fullNanos);
        }
    }
}
