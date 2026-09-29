//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.minusRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOfSortedKeys;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.unionRanges;
import static org.junit.Assert.assertEquals;

/**
 * The range oracles other tests compare against must themselves be right at the top of the key space, where adding one
 * to a range end wraps around.
 */
public class RangeOracleTest {

    private static final long MAX = Long.MAX_VALUE;

    @Test
    public void testUnionAtTheMaximumKey() {
        assertEquals("0-" + MAX + " ",
                render(unionRanges(List.of(new long[] {0, MAX}), List.of(new long[] {MAX, MAX}))));
        assertEquals("0-" + MAX + " ",
                render(unionRanges(List.of(new long[] {MAX - 1, MAX}), List.of(new long[] {0, MAX - 2}))));
        assertEquals("0-0 " + MAX + "-" + MAX + " ",
                render(unionRanges(List.of(new long[] {0, 0}), List.of(new long[] {MAX, MAX}))));
        assertEquals("1-3 5-9 ",
                render(unionRanges(List.of(new long[] {1, 2}, new long[] {5, 6}),
                        List.of(new long[] {3, 3}, new long[] {7, 9}))));
    }

    @Test
    public void testMinusAtTheMaximumKey() {
        // A range consumed through its end, where looking one key past it would wrap.
        assertEquals("", render(minusRanges(List.of(new long[] {MAX - 1, MAX}), List.of(new long[] {0, MAX}))));
        assertEquals((MAX - 2) + "-" + (MAX - 1) + " ",
                render(minusRanges(List.of(new long[] {MAX - 2, MAX}), List.of(new long[] {MAX, MAX}))));
        assertEquals("0-0 ", render(minusRanges(List.of(new long[] {0, 0}), List.of(new long[] {MAX, MAX}))));
    }

    @Test
    public void testMinusShapes() {
        // Nothing to take out: no range of b at all, then one that ends before a begins, then one that starts after.
        assertEquals("5-9 ", render(minusRanges(List.of(new long[] {5, 9}), List.of())));
        assertEquals("5-9 ", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {1, 4}))));
        assertEquals("5-9 ", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {10, 20}))));
        // A bite out of the front, so nothing sits ahead of it; out of the middle; out of the back.
        assertEquals("8-9 ", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {3, 7}))));
        assertEquals("5-5 9-9 ", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {6, 8}))));
        assertEquals("5-6 ", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {7, 12}))));
        // Consumed exactly, and consumed by a range that extends past both ends.
        assertEquals("", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {5, 9}))));
        assertEquals("", render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {1, 20}))));
        // Several bites out of one range, and one range of b spanning two ranges of a.
        assertEquals("5-5 7-7 9-9 ",
                render(minusRanges(List.of(new long[] {5, 9}), List.of(new long[] {6, 6}, new long[] {8, 8}))));
        assertEquals("1-2 20-20 ",
                render(minusRanges(List.of(new long[] {1, 5}, new long[] {10, 20}), List.of(new long[] {3, 19}))));
        // b is spent partway through a, so the later ranges of a survive whole.
        assertEquals("7-9 30-40 ",
                render(minusRanges(List.of(new long[] {5, 9}, new long[] {30, 40}), List.of(new long[] {4, 6}))));
    }

    @Test
    public void testSortedKeysAtTheMaximumKey() {
        assertEquals((MAX - 2) + "-" + MAX + " ", render(rangesOfSortedKeys(MAX - 2, MAX - 1, MAX)));
        assertEquals("0-1 " + MAX + "-" + MAX + " ", render(rangesOfSortedKeys(0, 1, MAX)));
        assertEquals("", render(rangesOfSortedKeys()));
    }
}
