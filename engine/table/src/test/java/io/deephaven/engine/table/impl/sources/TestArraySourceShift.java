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
                        assertEquals("delta=" + delta + ", ii=" + ii, (Long) expected(ii, delta), source.get(ii));
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
    public void testSetNullRange() {
        for (final boolean trackPrev : new boolean[] {false, true}) {
            final LongArraySource longs = new LongArraySource();
            final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
            longs.ensureCapacity(SIZE);
            objects.ensureCapacity(SIZE);
            for (int ii = 0; ii < SIZE; ++ii) {
                longs.set(ii, (long) ii);
                objects.set(ii, Long.toString(ii));
            }
            final Runnable nullAndCheck = () -> {
                longs.setNull(SHIFT_FIRST, SHIFT_LAST);
                objects.setNull(SHIFT_FIRST, SHIFT_LAST);
                for (long ii = 0; ii < SIZE; ++ii) {
                    final boolean nulled = ii >= SHIFT_FIRST && ii <= SHIFT_LAST;
                    assertEquals("ii=" + ii, nulled ? QueryConstants.NULL_LONG : ii, longs.getLong(ii));
                    assertEquals("ii=" + ii, nulled ? null : Long.toString(ii), objects.get(ii));
                    if (trackPrev) {
                        assertEquals("ii=" + ii, ii, longs.getPrevLong(ii));
                        assertEquals("ii=" + ii, Long.toString(ii), objects.getPrev(ii));
                    }
                }
            };
            if (trackPrev) {
                longs.startTrackingPrevValues();
                objects.startTrackingPrevValues();
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(nullAndCheck::run);
            } else {
                nullAndCheck.run();
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
