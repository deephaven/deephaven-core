//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * A search iterator tracks the key after the one it produced. When that key is {@link Long#MAX_VALUE} there is no key
 * after it, and stepping there anyway wraps to a negative value that compares as still inside the current range.
 */
public class RspSearchIteratorAtMaxKeyTest {

    private static final long MAX = Long.MAX_VALUE;
    private static final long TOP_BLOCK = MAX - BLOCK_SIZE + 1;

    private static WritableRowSet rspOf(final long[]... ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long[] r : ranges) {
            rb = rb.appendRangeUnsafe(r[0], r[1]);
        }
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    private static void walk(final WritableRowSet rs, final long expectedCount) {
        try (final RowSet.SearchIterator it = rs.searchIterator()) {
            long count = 0, last = -1;
            while (it.hasNext()) {
                last = it.nextLong();
                if (++count > expectedCount + 4) {
                    fail("search iteration did not stop: " + count + " keys, last was " + last);
                }
            }
            assertEquals("keys", expectedCount, count);
            assertEquals("last key", MAX, last);
            assertFalse("must be exhausted", it.hasNext());
        }
    }

    /** A single key at the very top. */
    @Test
    public void testSingletonAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {MAX, MAX})) {
            walk(rs, 1);
        }
    }

    /** A range ending at the last key. */
    @Test
    public void testRangeEndingAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {MAX - 4, MAX})) {
            walk(rs, 5);
        }
    }

    /** A full block span reaching the last key, preceded by another span. */
    @Test
    public void testFullBlockSpanToTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9}, new long[] {TOP_BLOCK, MAX})) {
            walk(rs, 5 + BLOCK_SIZE);
        }
    }

    /** advance past the last key must report exhaustion rather than a wrapped position. */
    @Test
    public void testAdvanceToTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9}, new long[] {MAX - 2, MAX});
                final RowSet.SearchIterator it = rs.searchIterator()) {
            assertEquals("advance lands on the last key", true, it.advance(MAX));
            assertEquals("current value", MAX, it.currentValue());
            assertFalse("nothing follows the last key", it.hasNext());
        }
    }
}
