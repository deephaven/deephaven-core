//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Timing checks that the row-set-consuming methods of {@link RowSetShiftData} gallop past the shift ranges that hold no
 * row keys, rather than visiting every shift range. Each compares against a single walk of the whole shift list in the
 * same run, with margins wide enough to absorb timing noise.
 */
@Category(OutOfBandTest.class)
public class RowSetShiftDataApplyTimingTest {

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

    /**
     * Visits a two-key row set, with one key in the first and one in the last of a million shift ranges, a hundred
     * times, once with every delta positive (the descending pass) and once with every delta negative (the ascending
     * pass), and compares each total with a single visit of a row set holding a key in every range. Between its two
     * keys each visit gallops across the empty shift ranges in about forty probes, so the hundred visits take a small
     * fraction of one full walk in either direction; stepping across the empty ranges one at a time would cost each
     * visit a walk of the whole shift list, several full walks in total. Each measurement is the fastest of four
     * repetitions, the first of which also warms up both paths.
     */
    @Test
    public void testForAllInRowSetGallopsAcrossEmptyRangesInBothDirections() {
        final int numRanges = 1_000_000;
        for (final long delta : new long[] {2, -2}) {
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            for (int ii = 0; ii < numRanges; ++ii) {
                builder.shiftRange(10L * ii + 3, 10L * ii + 5, delta);
            }
            final RowSetShiftData sd = builder.build();

            final RowSetBuilderSequential everyRange = RowSetFactory.builderSequential();
            for (int ii = 0; ii < numRanges; ++ii) {
                everyRange.appendKey(10L * ii + 4);
            }
            try (final RowSet fullRowSet = everyRange.build();
                    final RowSet endsRowSet = RowSetFactory.fromKeys(4, 10L * (numRanges - 1) + 4)) {
                final int numVisits = 100;
                final long[] calls = new long[1];
                long fullNanos = Long.MAX_VALUE;
                long endsNanos = Long.MAX_VALUE;
                for (int rep = 0; rep < 4; ++rep) {
                    calls[0] = 0;
                    final long fullStart = System.nanoTime();
                    sd.forAllInRowSet(fullRowSet, (key, keyDelta) -> ++calls[0]);
                    fullNanos = Math.min(fullNanos, System.nanoTime() - fullStart);
                    assertEquals(numRanges, calls[0]);

                    calls[0] = 0;
                    final long endsStart = System.nanoTime();
                    for (int ii = 0; ii < numVisits; ++ii) {
                        sd.forAllInRowSet(endsRowSet, (key, keyDelta) -> ++calls[0]);
                    }
                    endsNanos = Math.min(endsNanos, System.nanoTime() - endsStart);
                    assertEquals(2L * numVisits, calls[0]);
                }
                assertTrue("delta " + delta + ": " + numVisits + " visits took " + endsNanos + "ns, a full walk "
                        + fullNanos + "ns", endsNanos < fullNanos / 2);
            }
        }
    }
}
