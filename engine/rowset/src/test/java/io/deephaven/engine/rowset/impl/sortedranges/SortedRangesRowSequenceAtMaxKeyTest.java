//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Walking a slice of sorted ranges steps through each range up to its end. A range ending at {@link Long#MAX_VALUE} has
 * nothing past it, and stepping there anyway wraps to a negative key that compares as still inside the range.
 */
public class SortedRangesRowSequenceAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;

    private static WritableRowSet rowSetOf(final long[]... ranges) {
        final WritableRowSet rs = RowSetFactory.empty();
        for (final long[] r : ranges) {
            rs.insertRange(r[0], r[1]);
        }
        return rs;
    }

    private static List<Long> walk(final RowSequence seq, final long limit) {
        final List<Long> keys = new ArrayList<>();
        seq.forEachRowKey(k -> {
            keys.add(k);
            if (keys.size() > limit) {
                Assert.fail("walk did not stop: " + keys.size() + " keys, last was " + k);
            }
            return true;
        });
        return keys;
    }

    /** A slice whose last range ends at the last key, with an earlier range before it. */
    @Test
    public void testSliceEndingAtTheLastKey() {
        try (final WritableRowSet rs = rowSetOf(new long[] {5, 5}, new long[] {MAX - 3, MAX});
                final RowSequence seq = rs.getRowSequenceByKeyRange(4, MAX)) {
            assertEquals(List.of(5L, MAX - 3, MAX - 2, MAX - 1, MAX), walk(seq, 12));
        }
    }

    /** A slice that is only the range ending at the last key. */
    @Test
    public void testSliceOfOnlyTheTopRange() {
        try (final WritableRowSet rs = rowSetOf(new long[] {5, 5}, new long[] {MAX - 2, MAX});
                final RowSequence seq = rs.getRowSequenceByKeyRange(MAX - 2, MAX)) {
            assertEquals(List.of(MAX - 2, MAX - 1, MAX), walk(seq, 10));
        }
    }

    /** A mid-slice range ending at the last key, reached through the whole-sequence walk. */
    @Test
    public void testWholeSequenceEndingAtTheLastKey() {
        try (final WritableRowSet rs = rowSetOf(new long[] {5, 7}, new long[] {MAX - 1, MAX});
                final RowSequence seq = rs.getRowSequenceByPosition(0, rs.size())) {
            assertEquals(List.of(5L, 6L, 7L, MAX - 1, MAX), walk(seq, 12));
        }
    }

    /** Away from the top, for contrast. */
    @Test
    public void testSliceAwayFromTheTop() {
        try (final WritableRowSet rs = rowSetOf(new long[] {5, 5}, new long[] {100, 103});
                final RowSequence seq = rs.getRowSequenceByKeyRange(4, 103)) {
            assertEquals(List.of(5L, 100L, 101L, 102L, 103L), walk(seq, 12));
        }
    }
}
