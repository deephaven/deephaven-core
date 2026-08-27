//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Asking for zero keys yields an empty row sequence, whichever implementation backs the rowset.
 */
public class RowSequenceZeroLengthTest {

    private static WritableRowSet singleRange() {
        return new WritableRowSetImpl(SingleRange.make(5, 5));
    }

    private static WritableRowSet sortedRanges() {
        return new WritableRowSetImpl(SortedRanges.makeSingleRange(5, 5));
    }

    private static WritableRowSet rsp() {
        return new WritableRowSetImpl(RspBitmap.makeSingleRange(5, 5));
    }

    private static void assertEmptyAtZeroLength(final String what, final WritableRowSet rs) {
        try (final RowSequence seq = rs.getRowSequenceByPosition(0, 0)) {
            assertEquals(what + ": size", 0, seq.size());
            assertTrue(what + ": the walk completes without producing a key", seq.forEachRowKey(k -> {
                throw new AssertionError(what + ": produced key " + k);
            }));
            assertTrue(what + ": isEmpty", seq.isEmpty());
        }
    }

    @Test
    public void testZeroLengthFromPositionZero() {
        try (final WritableRowSet a = singleRange();
                final WritableRowSet b = sortedRanges();
                final WritableRowSet c = rsp()) {
            assertEmptyAtZeroLength("single range", a);
            assertEmptyAtZeroLength("sorted ranges", b);
            assertEmptyAtZeroLength("rsp", c);
        }
    }

    @Test
    public void testZeroLengthFromALaterPosition() {
        final WritableRowSet[] sets = {singleRange(), sortedRanges(), rsp()};
        final String[] names = {"single range", "sorted ranges", "rsp"};
        for (int i = 0; i < sets.length; ++i) {
            try (final WritableRowSet rs = sets[i]) {
                rs.insertRange(100, 104);
                try (final RowSequence seq = rs.getRowSequenceByPosition(2, 0)) {
                    assertEquals(names[i] + ": size", 0, seq.size());
                }
            }
        }
    }

    @Test
    public void testNegativeLengthIsEmpty() {
        try (final WritableRowSet rs = sortedRanges();
                final RowSequence seq = rs.getRowSequenceByPosition(0, -1)) {
            assertEquals("size", 0, seq.size());
        }
    }
}
