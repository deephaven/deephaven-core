//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import org.junit.Test;

import java.util.List;

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
    public void testSortedKeysAtTheMaximumKey() {
        assertEquals((MAX - 2) + "-" + MAX + " ", render(rangesOfSortedKeys(MAX - 2, MAX - 1, MAX)));
        assertEquals("0-1 " + MAX + "-" + MAX + " ", render(rangesOfSortedKeys(0, 1, MAX)));
        assertEquals("", render(rangesOfSortedKeys()));
    }
}
