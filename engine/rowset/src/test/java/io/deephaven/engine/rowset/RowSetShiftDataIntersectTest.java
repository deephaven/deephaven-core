//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * {@link RowSetShiftData#intersect(RowSet)} walks the shift ranges and the row set together. These check it against a
 * scan that tests every shift range for overlap.
 */
public class RowSetShiftDataIntersectTest {

    /** Retains every shift range that overlaps {@code rowSet}, testing each one. */
    private static RowSetShiftData intersectByScan(final RowSetShiftData sd, final RowSet rowSet) {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        for (int idx = 0; idx < sd.size(); ++idx) {
            if (rowSet.overlapsRange(sd.getBeginRange(idx), sd.getEndRange(idx))) {
                builder.shiftRange(sd.getBeginRange(idx), sd.getEndRange(idx), sd.getShiftDelta(idx));
            }
        }
        return builder.build();
    }

    private static void assertMatchesScan(final RowSetShiftData sd, final RowSet rowSet) {
        final RowSetShiftData expected = intersectByScan(sd, rowSet);
        final RowSetShiftData actual = sd.intersect(rowSet);
        final String context = "shift=" + sd + ", rowSet=" + rowSet;
        assertEquals(context, expected.size(), actual.size());
        for (int idx = 0; idx < expected.size(); ++idx) {
            assertEquals(context, expected.getBeginRange(idx), actual.getBeginRange(idx));
            assertEquals(context, expected.getEndRange(idx), actual.getEndRange(idx));
            assertEquals(context, expected.getShiftDelta(idx), actual.getShiftDelta(idx));
        }
    }

    private static RowSetShiftData shifts(final long... beginEndDelta) {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        for (int ii = 0; ii < beginEndDelta.length; ii += 3) {
            builder.shiftRange(beginEndDelta[ii], beginEndDelta[ii + 1], beginEndDelta[ii + 2]);
        }
        return builder.build();
    }

    @Test
    public void testEmpty() {
        try (final RowSet empty = RowSetFactory.empty();
                final RowSet some = RowSetFactory.fromKeys(1, 5, 9)) {
            assertMatchesScan(RowSetShiftData.EMPTY, some);
            assertMatchesScan(shifts(0, 10, 1), empty);
        }
    }

    @Test
    public void testSingleRowSetRangeSpanningSeveralShifts() {
        try (final RowSet rowSet = RowSetFactory.fromRange(15, 75)) {
            assertMatchesScan(shifts(2, 10, -1, 12, 20, 1, 30, 40, -2, 50, 60, 2, 70, 80, 1, 90, 100, 3), rowSet);
        }
    }

    @Test
    public void testRowSetKeysInGapsAndAtBoundaries() {
        final RowSetShiftData sd = shifts(10, 20, -5, 40, 50, 7, 80, 80, 3);
        try (final RowSet gapsOnly = RowSetFactory.fromKeys(0, 9, 21, 39, 51, 79, 81, 1000);
                final RowSet boundaries = RowSetFactory.fromKeys(10, 50, 80);
                final RowSet interiorOnly = RowSetFactory.fromKeys(15, 45)) {
            assertMatchesScan(sd, gapsOnly);
            assertMatchesScan(sd, boundaries);
            assertMatchesScan(sd, interiorOnly);
        }
    }

    @Test
    public void testRandomized() {
        final Random random = new Random(0);
        for (int iteration = 0; iteration < 2000; ++iteration) {
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            // enough ranges that sparse row sets gallop across long runs of non-overlapping shifts
            final int rangeCount = random.nextInt(200);
            long next = random.nextInt(5);
            for (int ii = 0; ii < rangeCount; ++ii) {
                final long begin = next + 2 + random.nextInt(10);
                final long end = begin + random.nextInt(10);
                // a gap of at least 2 on each side leaves room for a delta of +/-1 without overlap or coalescing
                builder.shiftRange(begin, end, (ii & 1) == 0 ? 1 : -1);
                next = end + 2;
            }
            final RowSetShiftData sd = builder.build();

            final RowSetBuilderSequential rowSetBuilder = RowSetFactory.builderSequential();
            final long keySpace = next + 20;
            final int density = 1 + random.nextInt(20);
            for (long key = 0; key < keySpace; ++key) {
                if (random.nextInt(density) == 0) {
                    rowSetBuilder.appendKey(key);
                }
            }
            try (final RowSet rowSet = rowSetBuilder.build()) {
                assertMatchesScan(sd, rowSet);
            }
        }
    }
}
