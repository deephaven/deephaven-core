package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import static org.junit.Assert.*;

public class RowSetShiftDataTest {
    @Test(expected = IllegalArgumentException.class)
    public void testPreOverlapOneLeft() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 1);
        builder.shiftRange(9, 18, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPreOverlapInsidePrevious() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 1);
        builder.shiftRange(1, 8, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPreOverlapInsideNext() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(1, 8, 2);
        builder.shiftRange(0, 9, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPreOverlapOneRight() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(9, 18, 2);
        builder.shiftRange(0, 9, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPreCrossNoOverlap() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(10, 19, 2);
        builder.shiftRange(0, 9, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPostOverlapPositiveShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 20);
        builder.shiftRange(10, 19, 19);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPostCrossPositiveShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 100);
        builder.shiftRange(10, 19, 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPostOverlapNegativeShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, -20);
        builder.shiftRange(10, 19, -21);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPostCrossNegativeShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, -10);
        builder.shiftRange(10, 19, -100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPostOverlapPolarShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 10);
        builder.shiftRange(20, 29, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPostCrossPolarShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 20);
        builder.shiftRange(20, 29, -20);
    }

    @Test
    public void testPositiveShiftSingleRun() {
        final long[] a = genArray(40);
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 9, 10);
        builder.shiftRange(10, 19, 10);
        builder.shiftRange(20, 29, 10);
        final RowSetShiftData shifted = builder.build();
        shifted.validate();
        shifted.apply(createMemMovCallback(a));
        for (int idx = 10; idx < a.length; ++idx) {
            assertEquals(idx - 10, a[idx]);
        }
    }

    @Test
    public void testNegativeShiftSingleRun() {
        final long[] a = genArray(40);
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(10, 19, -10);
        builder.shiftRange(20, 29, -10);
        builder.shiftRange(30, 39, -10);
        final RowSetShiftData shifted = builder.build();
        shifted.validate();
        shifted.apply(createMemMovCallback(a));
        for (int idx = 0; idx < a.length - 10; ++idx) {
            assertEquals(idx + 10, a[idx]);
        }
    }

    @Test
    public void testIgnoreZeroShifts() {
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(10, 19, 0);
        builder.shiftRange(20, 29, 0);
        builder.shiftRange(30, 39, 0);
        final RowSetShiftData shifted = builder.build();
        shifted.validate();
        assertEquals(0, shifted.size());
        final MutableBoolean anyShift = new MutableBoolean(false);
        shifted.apply((s, e, l) -> {
            anyShift.setTrue();
        });
        assertFalse(anyShift.getValue());
    }

    @Test
    public void testMemMovSafeLRL() {
        final long[] a = genArray(9);
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 0, 1);
        builder.shiftRange(1, 1, 1);
        builder.shiftRange(4, 4, -1);
        builder.shiftRange(5, 5, -1);
        builder.shiftRange(6, 6, 1);
        builder.shiftRange(7, 7, 1);
        final RowSetShiftData shifted = builder.build();
        shifted.validate();
        shifted.apply(createMemMovCallback(a));
        assertArrayEquals(fromValues(0, 0, 1, 4, 5, 5, 6, 6, 7), a);

        // test unapply
        final long[] b = genArray(9);
        shifted.unapply(createMemMovCallback(b));
        assertArrayEquals(fromValues(1, 2, 2, 3, 3, 4, 7, 8, 8), b);
    }

    @Test
    public void testMemMovSafeRLR() {
        final long[] a = genArray(9);
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(1, 1, -1);
        builder.shiftRange(2, 2, -1);
        builder.shiftRange(3, 3, 1);
        builder.shiftRange(4, 4, 1);
        builder.shiftRange(7, 7, -1);
        builder.shiftRange(8, 8, -1);
        final RowSetShiftData shifted = builder.build();
        shifted.validate();
        shifted.apply(createMemMovCallback(a));
        assertArrayEquals(fromValues(1, 2, 2, 3, 3, 4, 7, 8, 8), a);

        // test unapply
        final long[] b = genArray(9);
        shifted.unapply(createMemMovCallback(b));
        assertArrayEquals(fromValues(0, 0, 1, 4, 5, 5, 6, 6, 7), b);
    }

    @Test
    public void testMoveSingleItemRange() {
        final long[] a = genArray(10);
        final RowSetShiftData.Builder builder = newBuilder();
        builder.shiftRange(0, 0, 9);
        final RowSetShiftData shifted = builder.build();
        shifted.validate();
        shifted.apply(createMemMovCallback(a));
        assertArrayEquals(fromValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 0), a);

        // test unapply
        final long[] b = genArray(10);
        shifted.unapply(createMemMovCallback(b));
        assertArrayEquals(fromValues(9, 1, 2, 3, 4, 5, 6, 7, 8, 9), b);
    }

    @Test
    public void testSmartCoalescingBuilder1() {
        final RowSet rowSet = getScbTestIndex();

        final RowSetShiftData.Builder dumbBuilder = newBuilder();

        dumbBuilder.shiftRange(50, 150, -10);
        dumbBuilder.shiftRange(250, 350, -10);
        dumbBuilder.shiftRange(3950, 4050, 10);
        dumbBuilder.shiftRange(4150, 4250, 10);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(50, 150, -10);
        scb.shiftRange(250, 350, -10);
        scb.shiftRange(4150, 4250, 10);
        scb.shiftRange(3950, 4050, 10);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(didx, sidx);
    }

    @Test
    public void testSmartCoalescingBuilder2() {
        final RowSet rowSet = getScbTestIndex();

        final RowSetShiftData.Builder dumbBuilder = newBuilder();

        dumbBuilder.shiftRange(50, 150, -10);
        dumbBuilder.shiftRange(190, 210, -10);
        dumbBuilder.shiftRange(250, 350, -10);
        dumbBuilder.shiftRange(3950, 4050, 10);
        dumbBuilder.shiftRange(4090, 4110, 10);
        dumbBuilder.shiftRange(4150, 4250, 10);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(50, 150, -10);
        scb.shiftRange(190, 210, -10);
        scb.shiftRange(250, 350, -10);
        scb.shiftRange(4150, 4250, 10);
        scb.shiftRange(4090, 4110, 10);
        scb.shiftRange(3950, 4050, 10);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(2, sisd.size());

        assertEquals(didx, sidx);
    }


    @Test
    public void testSmartCoalescingBuilder3() {
        final RowSet rowSet = getScbTestIndex();

        final RowSetShiftData.Builder dumbBuilder = newBuilder();

        dumbBuilder.shiftRange(1000, 4000, 10);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(1000, 4000, 10);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(didx, sidx);
    }


    @Test
    public void testSmartCoalescingBuilder4() {
        final RowSet rowSet = getScbTestIndex();

        final RowSetShiftData.Builder dumbBuilder = newBuilder();
        dumbBuilder.shiftRange(20, 30, 10);
        dumbBuilder.shiftRange(45, 45, -1);
        dumbBuilder.shiftRange(50, 150, 10);
        dumbBuilder.shiftRange(250, 350, 10);
        dumbBuilder.shiftRange(600, 600, -1);
        dumbBuilder.shiftRange(3950, 4050, 10);
        dumbBuilder.shiftRange(4150, 4250, 10);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(20, 30, 10);
        scb.shiftRange(45, 45, -1);
        scb.shiftRange(250, 350, 10);
        scb.shiftRange(50, 150, 10);
        scb.shiftRange(600, 600, -1);
        scb.shiftRange(4150, 4250, 10);
        scb.shiftRange(3950, 4050, 10);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(didx, sidx);
    }

    @Test
    public void testSmartCoalescingBuilder5() {
        testSmartCoalescingBuilder5and6(getScbTestIndex());
    }

    @Test
    public void testSmartCoalescingBuilder6() {
        testSmartCoalescingBuilder5and6(RowSetFactory.empty());
    }

    private void testSmartCoalescingBuilder5and6(RowSet rowSet) {
        final RowSetShiftData.Builder dumbBuilder = newBuilder();
        dumbBuilder.shiftRange(30, 40, 10);
        dumbBuilder.shiftRange(50, 150, 10);
        dumbBuilder.shiftRange(250, 350, 10);
        dumbBuilder.shiftRange(4300, 4301, -1);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(250, 350, 10);
        scb.shiftRange(50, 150, 10);
        scb.shiftRange(30, 40, 10);
        scb.shiftRange(4300, 4301, -1);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(didx, sidx);
    }

    @Test
    public void testSmartCoalescingBuilder7() {
        final RowSet rowSet = RowSetFactory.fromKeys(1, 10, 13);

        final RowSetShiftData.Builder dumbBuilder = newBuilder();
        dumbBuilder.shiftRange(1, 2, 2);
        dumbBuilder.shiftRange(10, 12, 1);
        dumbBuilder.shiftRange(13, 13, 3);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(1, 2, 2);
        scb.shiftRange(13, 13, 3);
        scb.shiftRange(10, 12, 1);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(didx, sidx);
    }

    @Test
    public void testSmartCoalescingBuilder8() {
        final RowSet rowSet = RowSetFactory.fromKeys(0, 2, 16, 17, 23, 30, 35, 40);

        final RowSetShiftData.Builder dumbBuilder = newBuilder();
        dumbBuilder.shiftRange(5, 6, 1);
        dumbBuilder.shiftRange(7, 7, 2);
        dumbBuilder.shiftRange(9, 10, 1);
        dumbBuilder.shiftRange(11, 14, 2);
        dumbBuilder.shiftRange(16, 18, 1);
        dumbBuilder.shiftRange(24, 29, -1);
        dumbBuilder.shiftRange(31, 36, -2);
        dumbBuilder.shiftRange(38, 40, -3);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(16, 18, 1);
        scb.shiftRange(11, 14, 2);
        scb.shiftRange(9, 10, 1);
        scb.shiftRange(7, 7, 2);
        scb.shiftRange(5, 6, 1);
        scb.shiftRange(24, 29, -1);
        scb.shiftRange(31, 36, -2);
        scb.shiftRange(38, 40, -3);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        assertEquals(didx, sidx);
    }

    @Test
    public void testSmartCoalescingBuilder9() {
        final RowSet rowSet = RowSetFactory.fromKeys(0, 1, 6, 13, 20);

        final RowSetShiftData.Builder dumbBuilder = newBuilder();
        dumbBuilder.shiftRange(3, 7, 1);
        dumbBuilder.shiftRange(13, 14, 1);
        dumbBuilder.shiftRange(20, 27, -1);
        final RowSetShiftData disd = dumbBuilder.build();

        final RowSetShiftData.SmartCoalescingBuilder scb = new RowSetShiftData.SmartCoalescingBuilder(rowSet.copy());
        scb.shiftRange(3, 7, 1);
        scb.shiftRange(13, 14, 1);
        scb.shiftRange(20, 27, -1);
        final RowSetShiftData sisd = scb.build();

        final WritableRowSet didx = rowSet.copy();
        disd.apply(didx);
        final WritableRowSet sidx = rowSet.copy();
        sisd.apply(sidx);

        System.out.println(sisd);

        assertEquals(didx, sidx);
    }


    private RowSet getScbTestIndex() {
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        sequentialBuilder.appendKey(100);
        sequentialBuilder.appendKey(200);
        sequentialBuilder.appendKey(300);
        sequentialBuilder.appendKey(400);
        sequentialBuilder.appendKey(500);
        sequentialBuilder.appendKey(600);
        sequentialBuilder.appendRange(1000, 1100);
        sequentialBuilder.appendRange(2000, 2100);
        sequentialBuilder.appendRange(3000, 3100);
        sequentialBuilder.appendKey(4000);
        sequentialBuilder.appendKey(4100);
        sequentialBuilder.appendKey(4200);
        return sequentialBuilder.build();
    }

    // These tests don't actually need / desire an underlying RowSet.
    private RowSetShiftData.Builder newBuilder() {
        return new RowSetShiftData.Builder();
    }

    private long[] genArray(int size) {
        final long[] a = new long[size];
        for (int idx = 0; idx < a.length; ++idx) {
            a[idx] = idx;
        }
        return a;
    }

    private long[] fromValues(long... values) {
        return values;
    }

    private RowSetShiftData.Callback createMemMovCallback(final long[] arr) {
        return (start, end, delta) -> {
            final long dir = (delta > 0) ? -1 : 1;
            if (dir < 0) {
                long tmp = start;
                start = end;
                end = tmp;
            }
            for (long idx = start; idx != end + dir; idx += dir) {
                arr[(int) (idx + delta)] = arr[(int) idx];
            }
        };
    }
}
