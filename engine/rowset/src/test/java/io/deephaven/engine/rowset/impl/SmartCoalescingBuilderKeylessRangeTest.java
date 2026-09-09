//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * {@link RowSetShiftData.SmartCoalescingBuilder} drops a shift range that holds no pre-shift key: nothing moves, and
 * recording it would keep the ranges on either side of it from coalescing. Whether the keys lie before, after, or on
 * both sides of such a range, and whichever polarity the shift has, the range does not appear in the built shift data,
 * while the ranges that do hold keys are kept and coalesce as usual.
 */
public class SmartCoalescingBuilderKeylessRangeTest {

    /** {@code shifts} are {@code {begin, end, delta}} triples, applied in order. The builder closes {@code keys}. */
    private static RowSetShiftData build(final RowSet keys, final long[]... shifts) {
        final RowSetShiftData.SmartCoalescingBuilder smart = new RowSetShiftData.SmartCoalescingBuilder(keys);
        for (final long[] shift : shifts) {
            smart.shiftRange(shift[0], shift[1], shift[2]);
        }
        return smart.build();
    }

    private static RowSet keys(final long[]... ranges) {
        return RowSetTestCommon.rspOf(ranges);
    }

    @Test
    public void testForwardKeylessRangeBeforeKeys() {
        assertEquals(0, build(RowSetFactory.fromRange(100, 200), new long[] {50, 60, -1}).size());
    }

    @Test
    public void testForwardKeylessRangeAfterKeys() {
        assertEquals(0, build(RowSetFactory.fromRange(100, 200), new long[] {300, 400, -1}).size());
    }

    @Test
    public void testReversedKeylessRangeBeforeKeys() {
        assertEquals(0, build(RowSetFactory.fromRange(100, 200), new long[] {50, 60, 1}).size());
    }

    @Test
    public void testReversedKeylessRangeAfterKeys() {
        assertEquals(0, build(RowSetFactory.fromRange(100, 200), new long[] {300, 400, 1}).size());
    }

    @Test
    public void testForwardKeylessRangeBetweenKeys() {
        assertEquals(0, build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {50, 60, -1}, new long[] {300, 400, -1}, new long[] {600, 700, -1}).size());
    }

    @Test
    public void testReversedKeylessRangeBetweenKeys() {
        assertEquals(0, build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {600, 700, 1}, new long[] {300, 400, 1}, new long[] {50, 60, 1}).size());
    }

    /** With no key between the kept ranges, the dropped range does not stand in the way of coalescing them. */
    @Test
    public void testForwardRangesAroundKeylessRangeCoalesce() {
        assertEquals("{[100,550]-1}", build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {100, 200, -1}, new long[] {300, 400, -1}, new long[] {450, 550, -1}).toString());
    }

    @Test
    public void testReversedRangesAroundKeylessRangeCoalesce() {
        assertEquals("{[100,550]+1}", build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {450, 550, 1}, new long[] {300, 400, 1}, new long[] {100, 200, 1}).toString());
    }

    /** A key between the kept ranges keeps them apart, dropped range or not. */
    @Test
    public void testForwardRangesAroundKeylessRangeWithInterveningKeyStayApart() {
        assertEquals("{[100,200]-1,[450,550]-1}",
                build(keys(new long[] {100, 200}, new long[] {420, 420}, new long[] {500, 500}),
                        new long[] {100, 200, -1}, new long[] {300, 400, -1}, new long[] {450, 550, -1}).toString());
    }

    @Test
    public void testReversedRangesAroundKeylessRangeWithInterveningKeyStayApart() {
        assertEquals("{[100,200]+1,[450,550]+1}",
                build(keys(new long[] {100, 200}, new long[] {420, 420}, new long[] {500, 500}),
                        new long[] {450, 550, 1}, new long[] {300, 400, 1}, new long[] {100, 200, 1}).toString());
    }
}
