//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Covers {@link ShiftableColumnSource#shift(RowSetShiftData)} for the array sources, in both directions, with ranges
 * that overlap their destination, and with and without previous-value tracking.
 */
public class TestArraySourceShift {
    private static final int SIZE = 3000;
    private static final long SHIFT_FIRST = 1000;
    private static final long SHIFT_LAST = 2499;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testLongShift() {
        for (final boolean trackPrev : new boolean[] {false, true}) {
            for (final long delta : new long[] {-400, -1, 1, 400}) {
                final LongArraySource source = new LongArraySource();
                source.ensureCapacity(SIZE);
                for (int ii = 0; ii < SIZE; ++ii) {
                    source.set(ii, (long) ii);
                }
                shift(source, trackPrev, delta, () -> {
                    for (long ii = 0; ii < SIZE; ++ii) {
                        assertEquals("delta=" + delta + ", ii=" + ii, expected(ii, delta), source.getLong(ii));
                        if (trackPrev) {
                            assertEquals("delta=" + delta + ", ii=" + ii, ii, source.getPrevLong(ii));
                        }
                    }
                });
            }
        }
    }

    @Test
    public void testObjectShift() {
        for (final boolean trackPrev : new boolean[] {false, true}) {
            for (final long delta : new long[] {-400, -1, 1, 400}) {
                final ObjectArraySource<Long> source = new ObjectArraySource<>(Long.class);
                source.ensureCapacity(SIZE);
                for (int ii = 0; ii < SIZE; ++ii) {
                    source.set(ii, Long.valueOf(ii));
                }
                shift(source, trackPrev, delta, () -> {
                    for (long ii = 0; ii < SIZE; ++ii) {
                        // an object source clears the positions it moves values from and not into
                        final boolean vacated = ii >= SHIFT_FIRST && ii <= SHIFT_LAST
                                && (ii < SHIFT_FIRST + delta || ii > SHIFT_LAST + delta);
                        assertEquals("delta=" + delta + ", ii=" + ii, vacated ? null : (Long) expected(ii, delta),
                                source.get(ii));
                        if (trackPrev) {
                            assertEquals("delta=" + delta + ", ii=" + ii, (Long) ii, source.getPrev(ii));
                        }
                    }
                });
            }
        }
    }

    @Test
    public void testBooleanShift() {
        for (final boolean trackPrev : new boolean[] {false, true}) {
            for (final long delta : new long[] {-400, -1, 1, 400}) {
                final BooleanArraySource source = new BooleanArraySource();
                source.ensureCapacity(SIZE);
                for (int ii = 0; ii < SIZE; ++ii) {
                    source.set(ii, booleanFor(ii));
                }
                shift(source, trackPrev, delta, () -> {
                    for (long ii = 0; ii < SIZE; ++ii) {
                        assertEquals("delta=" + delta + ", ii=" + ii, booleanFor(expected(ii, delta)), source.get(ii));
                        if (trackPrev) {
                            assertEquals("delta=" + delta + ", ii=" + ii, booleanFor(ii), source.getPrev(ii));
                        }
                    }
                });
            }
        }
    }

    /**
     * Shift {@code source} and run {@code check}; with {@code trackPrev}, both happen within one update cycle, so that
     * previous values still hold the values from before the shift.
     */
    @Test
    public void testBlockAlignedShifts() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        final int size = 8 * blockSize;
        // whole blocks plus a partial block at the end of the range
        final long first = 2L * blockSize;
        final long last = 5L * blockSize + 100;
        for (final boolean trackPrev : new boolean[] {false, true}) {
            for (final boolean partlyRecorded : new boolean[] {false, true}) {
                if (partlyRecorded && !trackPrev) {
                    continue;
                }
                for (final long delta : new long[] {-2L * blockSize, -blockSize, blockSize, 2L * blockSize}) {
                    final LongArraySource longs = new LongArraySource();
                    final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
                    longs.ensureCapacity(size);
                    objects.ensureCapacity(size);
                    for (int ii = 0; ii < size; ++ii) {
                        longs.set(ii, (long) ii);
                        objects.set(ii, Long.toString(ii));
                    }
                    final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
                    builder.shiftRange(first, last, delta);
                    final RowSetShiftData shiftData = builder.build();
                    final String description = "trackPrev=" + trackPrev + ", partlyRecorded=" + partlyRecorded
                            + ", delta=" + delta;
                    final Runnable shiftAndCheck = () -> {
                        if (partlyRecorded) {
                            // previous values recorded for part of a block before it moves
                            for (long ii = first + 10; ii < first + 20; ++ii) {
                                longs.set(ii, -ii);
                                objects.set(ii, "changed" + ii);
                            }
                        }
                        longs.shift(shiftData);
                        objects.shift(shiftData);
                        for (long ii = first + delta; ii <= last + delta; ++ii) {
                            final long original = ii - delta;
                            final boolean changed = partlyRecorded && original >= first + 10 && original < first + 20;
                            assertEquals(description + ", ii=" + ii, changed ? -original : original, longs.getLong(ii));
                            assertEquals(description + ", ii=" + ii,
                                    changed ? "changed" + original : Long.toString(original), objects.get(ii));
                        }
                        if (trackPrev) {
                            // every position's previous value is its value before this cycle
                            for (long ii = 0; ii < size; ++ii) {
                                assertEquals(description + ", ii=" + ii, ii, longs.getPrevLong(ii));
                                assertEquals(description + ", ii=" + ii, Long.toString(ii), objects.getPrev(ii));
                            }
                        }
                        // positions outside both ranges keep their values
                        for (long ii = 0; ii < size; ++ii) {
                            final boolean inSource = ii >= first && ii <= last;
                            final boolean inDest = ii >= first + delta && ii <= last + delta;
                            if (!inSource && !inDest) {
                                assertEquals(description + ", ii=" + ii, ii, longs.getLong(ii));
                            }
                        }
                    };
                    if (trackPrev) {
                        longs.startTrackingPrevValues();
                        objects.startTrackingPrevValues();
                        final ControlledUpdateGraph updateGraph =
                                ExecutionContext.getContext().getUpdateGraph().cast();
                        updateGraph.runWithinUnitTestCycle(shiftAndCheck::run);
                    } else {
                        shiftAndCheck.run();
                    }
                }
            }
        }
    }

    @Test
    public void testShiftClearsVacatedObjects() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        for (final boolean trackPrev : new boolean[] {false, true}) {
            final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
            objects.ensureCapacity(2L * blockSize);
            for (int ii = 0; ii < 2 * blockSize; ++ii) {
                objects.set(ii, Long.toString(ii));
            }
            // single positions collapsing toward the start of the first block, as a sparse run does
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            for (long ii = 1; ii < 16; ++ii) {
                builder.shiftRange(8 * ii, 8 * ii, -7 * ii);
            }
            final RowSetShiftData shiftData = builder.build();
            final Runnable shiftAndCheck = () -> {
                objects.shift(shiftData);
                for (long ii = 1; ii < 16; ++ii) {
                    assertEquals("ii=" + ii, Long.toString(8 * ii), objects.get(ii));
                }
                for (long ii = 16; ii < 8 * 16; ++ii) {
                    // positions moved from and not moved into hold no references
                    final boolean vacated = ii % 8 == 0;
                    assertEquals("ii=" + ii, vacated ? null : Long.toString(ii), objects.get(ii));
                }
                if (trackPrev) {
                    for (long ii = 0; ii < 8 * 16; ++ii) {
                        assertEquals("ii=" + ii, Long.toString(ii), objects.getPrev(ii));
                    }
                }
            };
            if (trackPrev) {
                objects.startTrackingPrevValues();
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(shiftAndCheck::run);
            } else {
                shiftAndCheck.run();
            }
        }
    }

    @Test
    public void testReleaseBlocks() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        final LongArraySource longs = new LongArraySource();
        longs.ensureCapacity(4L * blockSize);
        for (int ii = 0; ii < 4 * blockSize; ++ii) {
            longs.set(ii, (long) ii);
        }
        // an unbounded range releases every block from the first one it covers, leaving the capacity alone
        longs.releaseBlocks(2L * blockSize, Long.MAX_VALUE);
        assertEquals(4L * blockSize, longs.getCapacity());
        assertEquals(null, longs.getBlocks()[2]);
        assertEquals(null, longs.getBlocks()[3]);
        assertEquals(2L * blockSize - 1, longs.getLong(2L * blockSize - 1));

        // a range within the capacity releases only the blocks it covers entirely
        longs.releaseBlocks(1, blockSize - 1);
        assertEquals(0L, longs.getLong(0));
        longs.releaseBlocks(0, blockSize - 1);
        assertEquals(null, longs.getBlocks()[0]);
        assertEquals(4L * blockSize, longs.getCapacity());
        assertEquals(blockSize, longs.getLong(blockSize));
    }

    @Test
    public void testMoveCopiesWholeBlocks() {
        // the sort kernels move values and then write into the positions moved from, so move must copy them
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        final int size = 4 * blockSize;
        for (final long[] move : new long[][] {{0, blockSize, 2L * blockSize}, {2L * blockSize, 0, 2L * blockSize},
                {0, 2L * blockSize, 2L * blockSize}}) {
            final long source = move[0];
            final long dest = move[1];
            final long length = move[2];
            final LongArraySource longs = new LongArraySource();
            final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
            longs.ensureCapacity(size);
            objects.ensureCapacity(size);
            for (int ii = 0; ii < size; ++ii) {
                longs.set(ii, (long) ii);
                objects.set(ii, Long.toString(ii));
            }
            longs.move(source, dest, length);
            objects.move(source, dest, length);
            for (long ii = 0; ii < size; ++ii) {
                final long expected = ii >= dest && ii < dest + length ? ii - dest + source : ii;
                assertEquals("source=" + source + ", dest=" + dest + ", ii=" + ii, expected, longs.getLong(ii));
                assertEquals("source=" + source + ", dest=" + dest + ", ii=" + ii, Long.toString(expected),
                        objects.get(ii));
                longs.set(ii, -ii);
                objects.set(ii, "overwritten");
            }
        }
    }

    @Test
    public void testShiftIntoBlockMovedFromBySameShift() {
        // One shift moves a block down by a whole block, and moves a later range into the block it moved from.
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        final int size = 3 * blockSize;
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(blockSize, 2L * blockSize - 1, -blockSize);
        builder.shiftRange(2L * blockSize + 10, 2L * blockSize + 19, -blockSize - 5);
        final RowSetShiftData shiftData = builder.build();
        for (final boolean trackPrev : new boolean[] {false, true}) {
            final LongArraySource longs = new LongArraySource();
            final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
            final BooleanArraySource booleans = new BooleanArraySource();
            longs.ensureCapacity(size);
            objects.ensureCapacity(size);
            booleans.ensureCapacity(size);
            for (int ii = 0; ii < size; ++ii) {
                longs.set(ii, (long) ii);
                objects.set(ii, Long.toString(ii));
                booleans.set(ii, booleanFor(ii));
            }
            final Runnable shiftAndCheck = () -> {
                longs.shift(shiftData);
                objects.shift(shiftData);
                booleans.shift(shiftData);
                for (long ii = 0; ii < blockSize; ++ii) {
                    assertEquals("trackPrev=" + trackPrev + ", ii=" + ii, ii + blockSize, longs.getLong(ii));
                    assertEquals(Long.toString(ii + blockSize), objects.get(ii));
                    assertEquals(booleanFor(ii + blockSize), booleans.get(ii));
                }
                for (long ii = blockSize + 5; ii < blockSize + 15; ++ii) {
                    assertEquals("trackPrev=" + trackPrev + ", ii=" + ii, ii + blockSize + 5, longs.getLong(ii));
                    assertEquals(Long.toString(ii + blockSize + 5), objects.get(ii));
                    assertEquals(booleanFor(ii + blockSize + 5), booleans.get(ii));
                }
                if (trackPrev) {
                    for (long ii = 0; ii < size; ++ii) {
                        assertEquals("ii=" + ii, ii, longs.getPrevLong(ii));
                        assertEquals(Long.toString(ii), objects.getPrev(ii));
                        assertEquals(booleanFor(ii), booleans.getPrev(ii));
                    }
                }
            };
            if (trackPrev) {
                longs.startTrackingPrevValues();
                objects.startTrackingPrevValues();
                booleans.startTrackingPrevValues();
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(shiftAndCheck::run);
            } else {
                shiftAndCheck.run();
            }
        }
    }

    @Test
    public void testReleasedBlocksAreRecycledClean() {
        // a released block goes back to the recycler, and a later allocation that reuses it starts clean
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        for (int round = 0; round < 3; ++round) {
            final LongArraySource written = new LongArraySource();
            final ObjectArraySource<String> writtenObjects = new ObjectArraySource<>(String.class);
            written.ensureCapacity(blockSize);
            writtenObjects.ensureCapacity(blockSize);
            for (int ii = 0; ii < blockSize; ++ii) {
                written.set(ii, 7L);
                writtenObjects.set(ii, "stale");
            }
            written.releaseBlocks(0, blockSize - 1);
            writtenObjects.releaseBlocks(0, blockSize - 1);

            final LongArraySource nullFilled = new LongArraySource();
            nullFilled.ensureCapacity(blockSize, true);
            final LongArraySource zeroFilled = new LongArraySource();
            zeroFilled.ensureCapacity(blockSize, false);
            final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
            objects.ensureCapacity(blockSize);
            for (int ii = 0; ii < blockSize; ++ii) {
                assertEquals(QueryConstants.NULL_LONG, nullFilled.getLong(ii));
                assertEquals(0, zeroFilled.getLong(ii));
                assertEquals(null, objects.get(ii));
            }
            nullFilled.releaseBlocks(0, blockSize - 1);
            zeroFilled.releaseBlocks(0, blockSize - 1);
            objects.releaseBlocks(0, blockSize - 1);
        }
    }

    @Test
    public void testEnsureCapacityLike() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        final DoubleArraySource template = new DoubleArraySource();
        template.ensureCapacity(4L * blockSize);
        template.releaseBlocks(blockSize, 3L * blockSize - 1);

        final LongArraySource counts = new LongArraySource();
        counts.ensureCapacityLike(template, true);
        assertEquals(template.getCapacity(), counts.getCapacity());
        assertEquals(null, counts.getBlocks()[1]);
        assertEquals(null, counts.getBlocks()[2]);
        assertEquals(QueryConstants.NULL_LONG, counts.getLong(0));
        assertEquals(QueryConstants.NULL_LONG, counts.getLong(4L * blockSize - 1));

        // releasing through the end of the capacity leaves the capacity alone, and the copy allocates none of those
        // blocks
        template.releaseBlocks(3L * blockSize, Long.MAX_VALUE);
        final LongArraySource later = new LongArraySource();
        later.ensureCapacityLike(template, false);
        assertEquals(4L * blockSize, later.getCapacity());
        assertEquals(null, later.getBlocks()[1]);
        assertEquals(null, later.getBlocks()[3]);
        assertEquals(0, later.getLong(blockSize - 1));
        // growing past the capacity allocates only the new blocks
        later.ensureCapacity(5L * blockSize, false);
        assertEquals(null, later.getBlocks()[3]);
        assertEquals(0, later.getLong(5L * blockSize - 1));
    }

    @Test
    public void testEmptyShift() {
        checkShift(RowSetShiftData.EMPTY, 100);
    }

    @Test
    public void testConsecutiveUpwardRanges() {
        // two ranges move up in a row, so they are applied as one run from the last to the first
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 199, 50);
        builder.shiftRange(300, 399, 50);
        checkShift(builder.build(), 1000);
    }

    @Test
    public void testAlternatingDirections() {
        // runs down, up, up, and down, each ending where the direction changes
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 199, -50);
        builder.shiftRange(300, 399, 50);
        builder.shiftRange(600, 699, 50);
        builder.shiftRange(900, 999, -30);
        checkShift(builder.build(), 1000);
    }

    @Test
    public void testUpwardMoveFartherThanLength() {
        // the destination does not overlap the source, so the range is copied forward
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 109, 500);
        checkShift(builder.build(), 1000);
    }

    /**
     * Apply {@code shiftData} to long, object, and boolean sources whose row key {@code ii} holds {@code ii}, with and
     * without previous-value tracking, and check every value, and every previous value when tracked. A row key that a
     * range moves away from and nothing moves onto keeps its value, except in the object source, which clears it.
     */
    private static void checkShift(final RowSetShiftData shiftData, final int size) {
        final long[] expected = new long[size];
        final boolean[] vacated = new boolean[size];
        for (int ii = 0; ii < size; ++ii) {
            expected[ii] = ii;
        }
        for (int ri = 0; ri < shiftData.size(); ++ri) {
            for (long ii = shiftData.getBeginRange(ri); ii <= shiftData.getEndRange(ri); ++ii) {
                vacated[(int) ii] = true;
            }
        }
        for (int ri = 0; ri < shiftData.size(); ++ri) {
            final long delta = shiftData.getShiftDelta(ri);
            for (long ii = shiftData.getBeginRange(ri); ii <= shiftData.getEndRange(ri); ++ii) {
                expected[(int) (ii + delta)] = ii;
                vacated[(int) (ii + delta)] = false;
            }
        }
        for (final boolean trackPrev : new boolean[] {false, true}) {
            final LongArraySource longs = new LongArraySource();
            final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
            final BooleanArraySource booleans = new BooleanArraySource();
            longs.ensureCapacity(size);
            objects.ensureCapacity(size);
            booleans.ensureCapacity(size);
            for (int ii = 0; ii < size; ++ii) {
                longs.set(ii, (long) ii);
                objects.set(ii, Long.toString(ii));
                booleans.set(ii, booleanFor(ii));
            }
            final Runnable shiftAndCheck = () -> {
                longs.shift(shiftData);
                objects.shift(shiftData);
                booleans.shift(shiftData);
                for (int ii = 0; ii < size; ++ii) {
                    final String description = "trackPrev=" + trackPrev + ", ii=" + ii;
                    assertEquals(description, expected[ii], longs.getLong(ii));
                    assertEquals(description, vacated[ii] ? null : Long.toString(expected[ii]), objects.get(ii));
                    assertEquals(description, booleanFor(expected[ii]), booleans.get(ii));
                    if (trackPrev) {
                        assertEquals(description, ii, longs.getPrevLong(ii));
                        assertEquals(description, Long.toString(ii), objects.getPrev(ii));
                        assertEquals(description, booleanFor(ii), booleans.getPrev(ii));
                    }
                }
            };
            if (trackPrev) {
                longs.startTrackingPrevValues();
                objects.startTrackingPrevValues();
                booleans.startTrackingPrevValues();
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(shiftAndCheck::run);
            } else {
                shiftAndCheck.run();
            }
        }
    }

    private static void shift(final ShiftableColumnSource<?> source, final boolean trackPrev, final long delta,
            final Runnable check) {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(SHIFT_FIRST, SHIFT_LAST, delta);
        final RowSetShiftData shiftData = builder.build();
        if (!trackPrev) {
            source.shift(shiftData);
            check.run();
            return;
        }
        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            source.shift(shiftData);
            check.run();
        });
    }

    /**
     * The value originally written at row key {@code ii} is {@code ii}; after the shift, a destination holds the value
     * shifted into it, and every other row key keeps its original value.
     */
    private static long expected(final long ii, final long delta) {
        if (ii >= SHIFT_FIRST + delta && ii <= SHIFT_LAST + delta) {
            return ii - delta;
        }
        return ii;
    }

    private static Boolean booleanFor(final long value) {
        return value % 3 == 0 ? null : value % 3 == 1;
    }
}
