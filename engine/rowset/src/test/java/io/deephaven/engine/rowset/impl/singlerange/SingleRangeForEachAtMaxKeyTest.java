//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.singlerange;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Walking a single range compares against its end and steps one past it to finish. When the end is
 * {@link Long#MAX_VALUE} that step wraps to a negative key, which compares as still being inside the range.
 */
public class SingleRangeForEachAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;

    private static WritableRowSet singleRangeOf(final long start, final long end) {
        return new WritableRowSetImpl(SingleRange.make(start, end));
    }

    private static void assertWalksTo(final WritableRowSet rs, final long expectedCount, final long expectedLast) {
        final long[] count = {0};
        final long[] last = {-1};
        final long limit = expectedCount + 4;
        rs.forEachRowKey(k -> {
            last[0] = k;
            if (++count[0] > limit) {
                Assert.fail("walk did not stop: " + count[0] + " keys, last was " + k);
            }
            return true;
        });
        assertEquals("keys", expectedCount, count[0]);
        assertEquals("last key", expectedLast, last[0]);
    }

    @Test
    public void testForEachRowKeyOverARangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 4, MAX)) {
            assertWalksTo(rs, 5, MAX);
        }
    }

    @Test
    public void testForEachRowKeyOverASingleKeyAtTheTop() {
        try (final WritableRowSet rs = singleRangeOf(MAX, MAX)) {
            assertWalksTo(rs, 1, MAX);
        }
    }

    @Test
    public void testForAllRowKeysOverARangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 2, MAX)) {
            final long[] count = {0};
            final long[] last = {-1};
            rs.forAllRowKeys(k -> {
                last[0] = k;
                if (++count[0] > 8) {
                    Assert.fail("walk did not stop: " + count[0] + " keys, last was " + k);
                }
            });
            assertEquals("keys", 3, count[0]);
            assertEquals("last key", MAX, last[0]);
        }
    }

    /** The same walk reached through a row sequence, which shares the loop by way of the mixin. */
    @Test
    public void testRowSequenceForEachRowKeyOverARangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = singleRangeOf(MAX - 4, MAX);
                final RowSequence seq = rs.getRowSequenceByKeyRange(MAX - 3, MAX)) {
            final long[] count = {0};
            final long[] last = {-1};
            seq.forEachRowKey(k -> {
                last[0] = k;
                if (++count[0] > 8) {
                    Assert.fail("walk did not stop: " + count[0] + " keys, last was " + k);
                }
                return true;
            });
            assertEquals("keys", 4, count[0]);
            assertEquals("last key", MAX, last[0]);
        }
    }

    /** A range away from the top, for contrast. */
    @Test
    public void testForEachRowKeyAwayFromTheTop() {
        try (final WritableRowSet rs = singleRangeOf(100, 104)) {
            assertWalksTo(rs, 5, 104);
        }
    }
}
