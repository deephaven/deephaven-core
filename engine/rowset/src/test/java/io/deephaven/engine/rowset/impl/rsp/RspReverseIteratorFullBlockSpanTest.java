//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Reverse iteration over a full block span walks keys down towards the span's first key. A span starting at key 0 is
 * the boundary case: stepping below its first key underflows, and an unsigned comparison reads that as an enormous
 * value rather than as "before the start".
 */
public class RspReverseIteratorFullBlockSpanTest {

    private static final long BS = BLOCK_SIZE;

    private static WritableRowSet rspOf(final long[]... ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (final long[] r : ranges) {
            rb = rb.appendRangeUnsafe(r[0], r[1]);
        }
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    /** Collect a reverse iteration, refusing to run away: the bug this covers never terminates. */
    private static List<Long> reverseKeys(final RowSet rs) {
        final List<Long> keys = new ArrayList<>();
        final long limit = rs.size() + 8;
        try (final RowSet.SearchIterator it = rs.reverseIterator()) {
            while (it.hasNext()) {
                keys.add(it.nextLong());
                if (keys.size() > limit) {
                    fail("reverse iteration did not terminate: emitted " + keys.size()
                            + " keys for a rowset of size " + rs.size() + ", last was " + keys.get(keys.size() - 1));
                }
            }
        }
        return keys;
    }

    private static List<Long> descendingKeysOf(final RowSet rs) {
        final List<Long> forward = new ArrayList<>();
        rs.forAllRowKeys(forward::add);
        final List<Long> out = new ArrayList<>(forward);
        java.util.Collections.reverse(out);
        return out;
    }

    @Test
    public void testSingleBlockFullSpanAtZero() {
        try (final WritableRowSet rs = rspOf(new long[] {0, BS - 1})) {
            assertEquals(descendingKeysOf(rs), reverseKeys(rs));
        }
    }

    @Test
    public void testMultiBlockFullSpanAtZero() {
        try (final WritableRowSet rs = rspOf(new long[] {0, 3 * BS - 1})) {
            assertEquals(descendingKeysOf(rs), reverseKeys(rs));
        }
    }

    /** A full block span at zero followed by another span: exhausting the first must advance, not run away. */
    @Test
    public void testFullSpanAtZeroFollowedByAnotherSpan() {
        try (final WritableRowSet rs = rspOf(new long[] {0, BS - 1}, new long[] {5 * BS + 7, 5 * BS + 9})) {
            assertEquals(descendingKeysOf(rs), reverseKeys(rs));
        }
    }

    /** A full block span not at zero, for contrast: this direction already worked. */
    @Test
    public void testFullSpanAwayFromZero() {
        try (final WritableRowSet rs = rspOf(new long[] {4 * BS, 6 * BS - 1})) {
            assertEquals(descendingKeysOf(rs), reverseKeys(rs));
        }
    }

    /** A partly-filled block at zero, so the span is a container rather than a full block span. */
    @Test
    public void testContainerAtZero() {
        try (final WritableRowSet rs = rspOf(new long[] {0, 40}, new long[] {2 * BS, 2 * BS + 5})) {
            assertEquals(descendingKeysOf(rs), reverseKeys(rs));
        }
    }
}
