//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Sorted ranges hold a few ranges of any width, so the position of a key can exceed what an {@code int} holds once a
 * range spans more than 2^31 keys. {@link io.deephaven.engine.rowset.RowSet#find(long)} answers the true position for
 * every key, present or absent, after such a range, and agrees with
 * {@link io.deephaven.engine.rowset.RowSet#get(long)}.
 */
public class SortedRangesFindAfterLongRangeTest {

    private static final long TWO_POW_32 = 1L << 32;

    private static void checkFind(final WritableRowSet rs) {
        assertTrue("fixture is backed by sorted ranges",
                ((WritableRowSetImpl) rs).getInnerSet() instanceof SortedRanges);
        assertEquals(TWO_POW_32 + 1 + 1 + 11, rs.size());
        // positions inside the first range
        assertEquals(TWO_POW_32, rs.find(TWO_POW_32));
        // the singleton after it sits at position 2^32 + 1
        assertEquals(TWO_POW_32 + 10, rs.get(TWO_POW_32 + 1));
        assertEquals("find of a present key after a range longer than Integer.MAX_VALUE",
                TWO_POW_32 + 1, rs.find(TWO_POW_32 + 10));
        assertEquals("find of an absent key after a range longer than Integer.MAX_VALUE",
                -(TWO_POW_32 + 2) - 1, rs.find(TWO_POW_32 + 15));
        assertEquals(TWO_POW_32 + 2 + 5, rs.find(TWO_POW_32 + 25));
        assertEquals("find of an absent key past the last range",
                -(TWO_POW_32 + 13) - 1, rs.find(TWO_POW_32 + 31));
        // find(get(p)) == p everywhere
        for (final long p : new long[] {0, 1, TWO_POW_32 - 1, TWO_POW_32, TWO_POW_32 + 1, TWO_POW_32 + 2,
                TWO_POW_32 + 12}) {
            assertEquals("find(get(" + p + "))", p, rs.find(rs.get(p)));
        }
    }

    @Test
    public void testFromSequentialBuilder() {
        final RowSetBuilderSequential b = RowSetFactory.builderSequential();
        b.appendRange(0, TWO_POW_32); // 2^32 + 1 keys
        b.appendKey(TWO_POW_32 + 10);
        b.appendRange(TWO_POW_32 + 20, TWO_POW_32 + 30);
        try (final WritableRowSet rs = b.build()) {
            checkFind(rs);
        }
    }

    @Test
    public void testFromSortedRanges() {
        try (final WritableRowSet rs = sortedRangesOf(
                new long[] {0, TWO_POW_32}, new long[] {TWO_POW_32 + 10, TWO_POW_32 + 10},
                new long[] {TWO_POW_32 + 20, TWO_POW_32 + 30})) {
            checkFind(rs);
        }
    }

    /** Two long ranges, so that the accumulated position passes 2^33 as well. */
    @Test
    public void testAfterTwoLongRanges() {
        final long secondStart = 2 * TWO_POW_32;
        final long secondEnd = secondStart + TWO_POW_32 + 5;
        final long key = secondEnd + 100;
        try (final WritableRowSet rs = sortedRangesOf(
                new long[] {0, TWO_POW_32}, new long[] {secondStart, secondEnd}, new long[] {key, key})) {
            final long expectedPosition = (TWO_POW_32 + 1) + (TWO_POW_32 + 6);
            assertEquals(expectedPosition + 1, rs.size());
            assertEquals(key, rs.get(expectedPosition));
            assertEquals(expectedPosition, rs.find(key));
            assertEquals(-expectedPosition - 1, rs.find(key - 1));
            assertEquals(-(expectedPosition + 1) - 1, rs.find(key + 1));
            assertEquals(TWO_POW_32 + 1 + 3, rs.find(secondStart + 3));
        }
    }
}
