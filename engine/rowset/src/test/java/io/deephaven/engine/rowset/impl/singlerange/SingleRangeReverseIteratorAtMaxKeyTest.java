//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.singlerange;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Reverse iteration over a single range starts one key past its end and walks down. When the end is
 * {@link Long#MAX_VALUE} that starting point wraps below the range, leaving nothing to iterate.
 */
public class SingleRangeReverseIteratorAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;

    private static WritableRowSet singleRangeOf(final long start, final long end) {
        return new WritableRowSetImpl(SingleRange.make(start, end));
    }

    private static List<Long> reverseKeys(final WritableRowSet rs) {
        final List<Long> keys = new ArrayList<>();
        try (final RowSet.SearchIterator it = rs.reverseIterator()) {
            while (it.hasNext()) {
                keys.add(it.nextLong());
                if (keys.size() > rs.size() + 4) {
                    throw new AssertionError("reverse iteration did not stop: " + keys.size() + " keys");
                }
            }
        }
        return keys;
    }

    @Test
    public void testReverseIterationOverARangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 3, MAX)) {
            assertEquals(List.of(MAX, MAX - 1, MAX - 2, MAX - 3), reverseKeys(rs));
        }
    }

    @Test
    public void testReverseIterationOverASingleKeyAtTheTop() {
        try (final WritableRowSet rs = singleRangeOf(MAX, MAX)) {
            assertEquals(List.of(MAX), reverseKeys(rs));
        }
    }

    @Test
    public void testReverseAdvanceWithinARangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 5, MAX);
                final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertTrue("advance to a key inside the range", it.advance(MAX - 2));
            assertEquals("current value", MAX - 2, it.currentValue());
            assertEquals("next going down", MAX - 3, it.nextLong());
        }
    }

    @Test
    public void testReverseAdvanceBelowARangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 5, MAX);
                final RowSet.SearchIterator it = rs.reverseIterator()) {
            assertFalse("advance below the range reports nothing", it.advance(MAX - 100));
        }
    }

    /** A range away from the top, for contrast. */
    @Test
    public void testReverseIterationAwayFromTheTop() {
        try (final WritableRowSet rs = singleRangeOf(100, 103)) {
            assertEquals(List.of(103L, 102L, 101L, 100L), reverseKeys(rs));
        }
    }
}
