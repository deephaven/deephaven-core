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

/**
 * Unlike {@link WritableRowSet#insert(RowSet)}, applying {@link WritableRowSet#insertWithShift} to the rowset itself is
 * not a no-op: it unions in a shifted copy of its own keys.
 */
public class InsertWithShiftSelfTest {

    private static void assertSelfShiftMatchesACopy(final String what, final WritableRowSet rs, final long shift) {
        final String expected;
        try (final WritableRowSet viaCopy = rs.copy();
                final WritableRowSet other = rs.copy()) {
            viaCopy.insertWithShift(shift, other);
            expected = viaCopy.toString();
        }
        rs.insertWithShift(shift, rs);
        assertEquals(what + ", shift " + shift, expected, rs.toString());
    }

    private static void checkAllImpls(final long shift) {
        try (final WritableRowSet single = new WritableRowSetImpl(SingleRange.make(5, 9));
                final WritableRowSet sorted = new WritableRowSetImpl(SortedRanges.makeSingleRange(5, 9));
                final WritableRowSet rsp = new WritableRowSetImpl(RspBitmap.makeSingleRange(5, 9))) {
            assertSelfShiftMatchesACopy("single range", single, shift);
            assertSelfShiftMatchesACopy("sorted ranges", sorted, shift);
            assertSelfShiftMatchesACopy("rsp", rsp, shift);
        }
    }

    @Test
    public void testSelfShiftWithinOneBlock() {
        checkAllImpls(100);
    }

    @Test
    public void testSelfShiftByAWholeBlock() {
        checkAllImpls(65536);
    }

    @Test
    public void testSelfShiftOverlapping() {
        checkAllImpls(2);
    }

    @Test
    public void testSelfShiftOfAMultiBlockRowSet() {
        try (final WritableRowSet rsp = new WritableRowSetImpl(RspBitmap.makeSingleRange(5, 9))) {
            rsp.insertRange(65536 * 3, 65536 * 3 + 10);
            rsp.insertRange(65536 * 7 + 1, 65536 * 7 + 3);
            assertSelfShiftMatchesACopy("multi-block rsp", rsp, 65536);
        }
    }
}
