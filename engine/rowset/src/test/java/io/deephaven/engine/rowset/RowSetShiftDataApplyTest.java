//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * {@link RowSetShiftData#apply(long)} bisects the shift ranges rather than scanning them. These check it against a
 * straightforward scan, including for keys landing in the gaps between ranges and outside them entirely.
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
}
