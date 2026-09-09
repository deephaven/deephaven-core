//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Slicing by position where the slice ends exactly on a block boundary inside a multi-block run. A paged implementation
 * decides how many spans the result needs separately from writing them, and the two must agree: a slot counted but not
 * written stays zeroed, and a zeroed slot decodes as the key zero.
 */
public class SubSetByPositionRangeBlockBoundaryTest {

    /** The first {@code count} keys of {@code src}, computed by walking its ranges. */
    private static String expectedPrefix(final RowSet src, final long count) {
        final List<String> out = new ArrayList<>();
        final long[] left = {count};
        src.forEachRowKeyRange((s, e) -> {
            final long take = Math.min(left[0], e - s + 1);
            if (take > 0) {
                out.add(s + "-" + (s + take - 1));
                left[0] -= take;
            }
            return left[0] > 0;
        });
        return String.join(" ", out);
    }

    private static String ranges(final RowSet rs) {
        final List<String> out = new ArrayList<>();
        rs.forEachRowKeyRange((s, e) -> {
            out.add(s + "-" + e);
            return true;
        });
        return String.join(" ", out);
    }

    private static void checkSlices(final String what, final WritableRowSet src, final long... endPositions) {
        for (final long endPos : endPositions) {
            final String where = what + " subSetByPositionRange(0, " + endPos + ")";
            try (final WritableRowSet sub = src.subSetByPositionRange(0, endPos)) {
                ((WritableRowSetImpl) sub).getInnerSet().ixValidate(where);
                assertEquals(where + " contents", expectedPrefix(src, endPos), ranges(sub));
                assertEquals(where + " size", Math.min(endPos, src.size()), sub.size());
            }
        }
    }

    /** Every position at, either side of, and well inside each block boundary of a multi-block run. */
    private static long[] boundaryPositions(final long lead, final long blocks) {
        final List<Long> out = new ArrayList<>();
        for (long b = 1; b <= blocks; ++b) {
            for (final long delta : new long[] {-1, 0, 1}) {
                final long p = lead + b * BLOCK_SIZE + delta;
                if (p > 0) {
                    out.add(p);
                }
            }
        }
        out.add(lead + blocks * BLOCK_SIZE / 2);
        return out.stream().mapToLong(Long::longValue).toArray();
    }

    /** A leading singleton then a multi-block run: only the paged and sorted-ranges forms can hold this. */
    @Test
    public void testSliceOfALeadingKeyPlusAMultiBlockRun() {
        final RspBitmap rsp = RspBitmap.makeSingleRange(5, 5);
        rsp.addRangeUnsafeNoWriteCheck(BLOCK_SIZE, 4 * BLOCK_SIZE - 1);
        rsp.finishMutations();
        try (final WritableRowSet paged = new WritableRowSetImpl(rsp)) {
            assertBackedBy("rsp", paged, "Rsp");
            checkSlices("rsp", paged, boundaryPositions(1, 3));
        }
        try (final WritableRowSet sorted = new WritableRowSetImpl(
                SortedRanges.makeSingleRange(5, 5).addRange(BLOCK_SIZE, 4 * BLOCK_SIZE - 1))) {
            assertBackedBy("sorted ranges", sorted, "SortedRanges");
            checkSlices("sorted ranges", sorted, boundaryPositions(1, 3));
        }
    }

    /** A single multi-block run, which every backing can hold. */
    @Test
    public void testSliceOfAMultiBlockRunOnEveryBacking() {
        final long start = BLOCK_SIZE;
        final long end = 4 * BLOCK_SIZE - 1;
        try (final WritableRowSet single = new WritableRowSetImpl(SingleRange.make(start, end))) {
            assertBackedBy("single range", single, "SingleRange");
            checkSlices("single range", single, boundaryPositions(0, 3));
        }
        try (final WritableRowSet sorted = new WritableRowSetImpl(SortedRanges.makeSingleRange(start, end))) {
            assertBackedBy("sorted ranges", sorted, "SortedRanges");
            checkSlices("sorted ranges", sorted, boundaryPositions(0, 3));
        }
        try (final WritableRowSet paged = new WritableRowSetImpl(RspBitmap.makeSingleRange(start, end))) {
            assertBackedBy("rsp", paged, "Rsp");
            checkSlices("rsp", paged, boundaryPositions(0, 3));
        }
    }

    /** A run long enough that its span object is a boxed length rather than the marker. */
    @Test
    public void testSliceOfARunTooLongForTheSpanMarker() {
        final RspBitmap rsp = RspBitmap.makeSingleRange(3, 3);
        rsp.addRangeUnsafeNoWriteCheck(BLOCK_SIZE, BLOCK_SIZE + (1L << 33) - 1);
        rsp.finishMutations();
        try (final WritableRowSet paged = new WritableRowSetImpl(rsp)) {
            // More than 0xFFFF blocks, so the span's length cannot be held in the marker's bits and the span
            // object is a boxed Long instead.
            assertTrue("the run spans more than 0xFFFF blocks", paged.size() > 0xFFFFL * BLOCK_SIZE / 2);
            checkSlices("boxed span", paged, boundaryPositions(1, 3));
        }
    }

}
