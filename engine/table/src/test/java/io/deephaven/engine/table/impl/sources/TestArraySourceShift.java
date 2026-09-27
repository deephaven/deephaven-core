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
    public void testWholeBlockShifts() {
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
                        // Vacated positions hold no values: a block vacated entirely is left unallocated, and the
                        // rest of a partly vacated block can be written.
                        for (long ii = first; ii <= last; ++ii) {
                            if (ii >= first + delta && ii <= last + delta) {
                                continue;
                            }
                            final long blockFirst = ii & ~(long) (blockSize - 1);
                            final long blockLast = blockFirst + blockSize - 1;
                            final boolean blockVacated = blockFirst >= first && blockLast <= last
                                    && (blockLast < first + delta || blockFirst > last + delta);
                            final long key = ii;
                            if (blockVacated) {
                                assertThrows(NullPointerException.class, () -> longs.getLong(key));
                            } else {
                                longs.set(key, 7L);
                                objects.set(key, "seven");
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
    public void testWholeBlockShiftFromReleasedBlock() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        for (final boolean trackPrev : new boolean[] {false, true}) {
            final LongArraySource longs = new LongArraySource();
            longs.ensureCapacity(4L * blockSize);
            for (int ii = 0; ii < 4 * blockSize; ++ii) {
                longs.set(ii, (long) ii);
            }
            // the first block is released, then the rest of the source moves down over it
            longs.releaseBlocks(0, blockSize - 1);
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(blockSize, 4L * blockSize - 1, -blockSize);
            final RowSetShiftData shiftData = builder.build();
            final Runnable shiftAndCheck = () -> {
                longs.shift(shiftData);
                for (long ii = 0; ii < 3L * blockSize; ++ii) {
                    assertEquals("ii=" + ii, ii + blockSize, longs.getLong(ii));
                }
            };
            if (trackPrev) {
                longs.startTrackingPrevValues();
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(shiftAndCheck::run);
            } else {
                shiftAndCheck.run();
            }
            assertVacatedLastBlockReallocates(longs);
        }
    }

    @Test
    public void testWholeBlockShiftVacatesReleasedBlock() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        for (final boolean trackPrev : new boolean[] {false, true}) {
            final LongArraySource longs = new LongArraySource();
            longs.ensureCapacity(4L * blockSize);
            for (int ii = 0; ii < 4 * blockSize; ++ii) {
                longs.set(ii, (long) ii);
            }
            // the last block is released as well as the first, so the block the move vacates holds no array
            longs.releaseBlocks(0, blockSize - 1);
            longs.releaseBlocks(3L * blockSize, 4L * blockSize - 1);
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(blockSize, 4L * blockSize - 1, -blockSize);
            final RowSetShiftData shiftData = builder.build();
            final Runnable shiftAndCheck = () -> {
                longs.shift(shiftData);
                for (long ii = 0; ii < 2L * blockSize; ++ii) {
                    assertEquals("ii=" + ii, ii + blockSize, longs.getLong(ii));
                }
            };
            if (trackPrev) {
                longs.startTrackingPrevValues();
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(shiftAndCheck::run);
            } else {
                shiftAndCheck.run();
            }
            // releasing the last block reached the end of the capacity, which shrank to exclude it
            assertEquals(3L * blockSize, longs.getCapacity());
            longs.ensureCapacity(4L * blockSize);
            assertEquals(QueryConstants.NULL_LONG, longs.getLong(3L * blockSize));
            longs.set(4L * blockSize - 1, 7L);
            assertEquals(7L, longs.getLong(4L * blockSize - 1));
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
    public void testReleaseBlocksThroughEnd() {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        final LongArraySource longs = new LongArraySource();
        longs.ensureCapacity(4L * blockSize);
        for (int ii = 0; ii < 4 * blockSize; ++ii) {
            longs.set(ii, (long) ii);
        }
        // an unbounded range releases every block from the first one it covers, and shrinks the capacity to them
        longs.releaseBlocks(2L * blockSize, Long.MAX_VALUE);
        assertEquals(2L * blockSize, longs.getCapacity());
        assertEquals(null, longs.getBlocks()[2]);
        assertEquals(null, longs.getBlocks()[3]);
        assertEquals(2L * blockSize - 1, longs.getLong(2L * blockSize - 1));

        // a range within the capacity releases only the blocks it covers entirely
        longs.releaseBlocks(1, blockSize - 1);
        assertEquals(2L * blockSize, longs.getCapacity());
        longs.releaseBlocks(0, blockSize - 1);
        assertEquals(null, longs.getBlocks()[0]);
        assertEquals(2L * blockSize, longs.getCapacity());
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
    public void testShiftIntoBlockVacatedBySameShift() {
        // One shift moves a whole block down, leaving its block unallocated, and moves a later range element-wise into
        // that block. The element-wise move allocates the block again.
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

        // a template released through the end of its capacity has a smaller capacity, which growing allocates again
        template.releaseBlocks(3L * blockSize, Long.MAX_VALUE);
        final LongArraySource later = new LongArraySource();
        later.ensureCapacityLike(template, false);
        assertEquals(3L * blockSize, later.getCapacity());
        assertEquals(null, later.getBlocks()[1]);
        assertEquals(0, later.getLong(blockSize - 1));
        later.ensureCapacity(4L * blockSize, false);
        assertEquals(0, later.getLong(4L * blockSize - 1));
    }

    /**
     * The move down left the last of four blocks unallocated. Once it is released through the end of the capacity,
     * after the cycle, ensuring the capacity allocates it again, null-filled.
     */
    private static void assertVacatedLastBlockReallocates(final LongArraySource longs) {
        final int blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        assertThrows(NullPointerException.class, () -> longs.getLong(3L * blockSize));
        longs.releaseBlocks(3L * blockSize, Long.MAX_VALUE);
        assertEquals(3L * blockSize, longs.getCapacity());
        longs.ensureCapacity(4L * blockSize);
        assertEquals(QueryConstants.NULL_LONG, longs.getLong(3L * blockSize));
        longs.set(4L * blockSize - 1, 7L);
        assertEquals(7L, longs.getLong(4L * blockSize - 1));
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
