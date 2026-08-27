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
import static org.junit.Assert.fail;

/**
 * Forward iteration over a full block span reaching {@link Long#MAX_VALUE} must stop there. Stepping one past the last
 * key wraps to a negative value, which compares as still being inside the span.
 */
public class RspIteratorAtMaxKeyTest {

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

    @Test
    public void testIteratorStopsAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX});
                final RowSet.Iterator it = rs.iterator()) {
            long count = 0, last = -1;
            while (it.hasNext()) {
                last = it.nextLong();
                if (++count > rs.size() + 4) {
                    fail("iteration did not stop: " + count + " keys, last was " + last);
                }
            }
            assertEquals("keys", BLOCK_SIZE, count);
            assertEquals("last key", MAX, last);
        }
    }

    @Test
    public void testIteratorForEachLongStopsAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {TOP_BLOCK, MAX});
                final RowSet.Iterator it = rs.iterator()) {
            final long[] count = {0};
            final long[] last = {-1};
            final long limit = rs.size() + 4;
            it.forEachLong(v -> {
                last[0] = v;
                return ++count[0] <= limit;
            });
            assertEquals("keys", BLOCK_SIZE, count[0]);
            assertEquals("last key", MAX, last[0]);
        }
    }

    /** A span reaching the top preceded by another, so exhausting it must move on rather than run away. */
    @Test
    public void testIteratorAcrossASpanEndingAtTheLastKey() {
        try (final WritableRowSet rs = rspOf(new long[] {5, 9}, new long[] {TOP_BLOCK, MAX});
                final RowSet.Iterator it = rs.iterator()) {
            long count = 0, last = -1;
            while (it.hasNext()) {
                last = it.nextLong();
                if (++count > rs.size() + 4) {
                    fail("iteration did not stop: " + count + " keys, last was " + last);
                }
            }
            assertEquals("keys", 5 + BLOCK_SIZE, count);
            assertEquals("last key", MAX, last);
        }
    }
}
