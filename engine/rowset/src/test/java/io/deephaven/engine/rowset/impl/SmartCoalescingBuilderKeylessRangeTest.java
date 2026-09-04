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

    private static String render(final RowSetShiftData shiftData) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < shiftData.size(); ++i) {
            sb.append('[').append(shiftData.getBeginRange(i)).append(',').append(shiftData.getEndRange(i)).append("]")
                    .append(shiftData.getShiftDelta(i) < 0 ? "" : "+").append(shiftData.getShiftDelta(i)).append(' ');
        }
        return sb.toString();
    }

    private static RowSet keys(final long[]... ranges) {
        return RowSetTestCommon.rspOf(ranges);
    }

    @Test
    public void testForwardKeylessRangeBeforeKeys() {
        assertEquals("", render(build(RowSetFactory.fromRange(100, 200), new long[] {50, 60, -1})));
    }

    @Test
    public void testForwardKeylessRangeAfterKeys() {
        assertEquals("", render(build(RowSetFactory.fromRange(100, 200), new long[] {300, 400, -1})));
    }

    @Test
    public void testReversedKeylessRangeBeforeKeys() {
        assertEquals("", render(build(RowSetFactory.fromRange(100, 200), new long[] {50, 60, 1})));
    }

    @Test
    public void testReversedKeylessRangeAfterKeys() {
        assertEquals("", render(build(RowSetFactory.fromRange(100, 200), new long[] {300, 400, 1})));
    }

    @Test
    public void testForwardKeylessRangeBetweenKeys() {
        assertEquals("", render(build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {50, 60, -1}, new long[] {300, 400, -1}, new long[] {600, 700, -1})));
    }

    @Test
    public void testReversedKeylessRangeBetweenKeys() {
        assertEquals("", render(build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {600, 700, 1}, new long[] {300, 400, 1}, new long[] {50, 60, 1})));
    }

    /** With no key between the kept ranges, the dropped range does not stand in the way of coalescing them. */
    @Test
    public void testForwardRangesAroundKeylessRangeCoalesce() {
        assertEquals("[100,550]-1 ", render(build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {100, 200, -1}, new long[] {300, 400, -1}, new long[] {450, 550, -1})));
    }

    @Test
    public void testReversedRangesAroundKeylessRangeCoalesce() {
        assertEquals("[100,550]+1 ", render(build(keys(new long[] {100, 200}, new long[] {500, 500}),
                new long[] {450, 550, 1}, new long[] {300, 400, 1}, new long[] {100, 200, 1})));
    }

    /** A key between the kept ranges keeps them apart, dropped range or not. */
    @Test
    public void testForwardRangesAroundKeylessRangeWithInterveningKeyStayApart() {
        assertEquals("[100,200]-1 [450,550]-1 ",
                render(build(keys(new long[] {100, 200}, new long[] {420, 420}, new long[] {500, 500}),
                        new long[] {100, 200, -1}, new long[] {300, 400, -1}, new long[] {450, 550, -1})));
    }

    @Test
    public void testReversedRangesAroundKeylessRangeWithInterveningKeyStayApart() {
        assertEquals("[100,200]+1 [450,550]+1 ",
                render(build(keys(new long[] {100, 200}, new long[] {420, 420}, new long[] {500, 500}),
                        new long[] {450, 550, 1}, new long[] {300, 400, 1}, new long[] {100, 200, 1})));
    }
}
