//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Row keys are non-negative, so a shift window that begins below zero, or whose post-shift image begins below zero or
 * ends past {@link Long#MAX_VALUE}, does not map its whole extent onto keys, even when the rest of it holds keys that
 * could move. Both builders reject the whole window with an {@link IllegalArgumentException} rather than storing it,
 * which lets readers of shift data add {@code begin + delta} and {@code end + delta} without guarding against overflow.
 * Windows that end exactly at either edge of the key space are valid.
 */
public class RowSetShiftDataKeySpaceTest {

    private static final long MAX = Long.MAX_VALUE;

    private static RowSetShiftData.SmartCoalescingBuilder smartBuilder() {
        return new RowSetShiftData.SmartCoalescingBuilder(RowSetFactory.fromRange(0, MAX));
    }

    @Test
    public void testBuilderRejectsWindowBeginningBelowZero() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(-5, 10, 3));
    }

    /** A zero delta moves nothing, but the range is still checked before it is discarded. */
    @Test
    public void testBuilderRejectsWindowBeginningBelowZeroWithZeroDelta() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(-1, 5, 0));
    }

    @Test
    public void testBuilderRejectsShiftBelowZero() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(0, 300, -3));
    }

    @Test
    public void testBuilderRejectsShiftPastMaximum() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(MAX - 400, MAX, 3));
    }

    @Test
    public void testBuilderRejectsShiftPastMaximumByFullRange() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(10, 20, MAX));
    }

    @Test
    public void testBuilderAcceptsShiftLandingOnZero() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(3, 300, -3);
        final RowSetShiftData shiftData = builder.build();
        shiftData.validate();
        assertEquals("{[3,300]-3}", shiftData.toString());
    }

    @Test
    public void testBuilderAcceptsShiftLandingOnMaximum() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(MAX - 400, MAX - 3, 3);
        final RowSetShiftData shiftData = builder.build();
        shiftData.validate();
        assertEquals("{[" + (MAX - 400) + "," + (MAX - 3) + "]+3}", shiftData.toString());
    }

    @Test
    public void testSmartCoalescingBuilderRejectsWindowBeginningBelowZero() {
        try (final RowSetShiftData.SmartCoalescingBuilder builder = smartBuilder()) {
            assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(-5, 10, 3));
        }
    }

    @Test
    public void testSmartCoalescingBuilderRejectsWindowBeginningBelowZeroWithZeroDelta() {
        try (final RowSetShiftData.SmartCoalescingBuilder builder = smartBuilder()) {
            assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(-1, 5, 0));
        }
    }

    @Test
    public void testSmartCoalescingBuilderRejectsShiftBelowZero() {
        try (final RowSetShiftData.SmartCoalescingBuilder builder = smartBuilder()) {
            assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(0, 300, -3));
        }
    }

    @Test
    public void testSmartCoalescingBuilderRejectsShiftPastMaximum() {
        try (final RowSetShiftData.SmartCoalescingBuilder builder = smartBuilder()) {
            assertThrows(IllegalArgumentException.class, () -> builder.shiftRange(MAX - 400, MAX, 3));
        }
    }

    @Test
    public void testSmartCoalescingBuilderAcceptsShiftLandingOnMaximum() {
        try (final RowSetShiftData.SmartCoalescingBuilder builder = smartBuilder()) {
            builder.shiftRange(MAX - 400, MAX - 3, 3);
            final RowSetShiftData shiftData = builder.build();
            shiftData.validate();
            assertEquals("{[" + (MAX - 400) + "," + (MAX - 3) + "]+3}", shiftData.toString());
        }
    }

    /** Offsetting the window itself overflows. */
    @Test
    public void testUnapplyRejectsOffsetOverflowingTheWindow() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 300, 3);
        final RowSetShiftData shiftData = builder.build();
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(103, 303)) {
            assertThrows(IllegalArgumentException.class, () -> shiftData.unapply(rowSet, MAX));
        }
    }

    /** The offset window fits, but its post-shift image ends past the maximum. */
    @Test
    public void testUnapplyRejectsOffsetCarryingTheShiftPastMaximum() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 300, 3);
        final RowSetShiftData shiftData = builder.build();
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(103, 303)) {
            assertThrows(IllegalArgumentException.class, () -> shiftData.unapply(rowSet, MAX - 301));
        }
    }

    /** The offset window begins below zero, though its post-shift image does not. */
    @Test
    public void testUnapplyRejectsOffsetCarryingTheWindowBelowZero() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 300, 3);
        final RowSetShiftData shiftData = builder.build();
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(2, 202)) {
            assertThrows(IllegalArgumentException.class, () -> shiftData.unapply(rowSet, -101));
        }
    }

    @Test
    public void testUnapplyAcceptsOffsetLandingTheWindowOnZero() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 300, 3);
        final RowSetShiftData shiftData = builder.build();
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(3, 203);
                final WritableRowSet expected = RowSetFactory.fromRange(0, 200)) {
            shiftData.unapply(rowSet, -100);
            assertEquals(expected, rowSet);
        }
    }

    @Test
    public void testStaticApplyShiftRejectsShiftPastMaximum() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(MAX - 300, MAX - 100)) {
            assertThrows(IllegalArgumentException.class,
                    () -> RowSetShiftData.applyShift(rowSet, MAX - 400, MAX, 3));
        }
    }

    @Test
    public void testStaticUnapplyShiftRejectsShiftBelowZero() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(97, 297)) {
            assertThrows(IllegalArgumentException.class,
                    () -> RowSetShiftData.unapplyShift(rowSet, 0, 300, -3));
        }
    }

    @Test
    public void testStaticApplyShiftRejectsRangeEndingBeforeItBegins() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(100, 300)) {
            assertThrows(IllegalArgumentException.class, () -> RowSetShiftData.applyShift(rowSet, 300, 100, 3));
        }
    }

    @Test
    public void testStaticUnapplyShiftRejectsRangeEndingBeforeItBegins() {
        try (final WritableRowSet rowSet = RowSetFactory.fromRange(103, 303)) {
            assertThrows(IllegalArgumentException.class, () -> RowSetShiftData.unapplyShift(rowSet, 300, 100, 3));
        }
    }

    /** The rowset ends before the first window, so no traversal reaches the second; the offset is rejected anyway. */
    @Test
    public void testUnapplyRejectsOffsetForWindowBeyondTheRowSet() {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(100, 200, 3);
        builder.shiftRange(MAX - 500, MAX - 400, 3);
        final RowSetShiftData shiftData = builder.build();
        try (final WritableRowSet rowSet = RowSetFactory.empty()) {
            assertThrows(IllegalArgumentException.class, () -> shiftData.unapply(rowSet, 400));
        }
    }
}
