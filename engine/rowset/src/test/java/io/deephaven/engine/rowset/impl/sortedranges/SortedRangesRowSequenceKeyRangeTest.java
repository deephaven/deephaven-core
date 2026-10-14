//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.sortedranges;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link SortedRanges#getRowSequenceByKeyRange} over key ranges that select nothing, and over every valid key range of
 * a few representative shapes.
 */
public class SortedRangesRowSequenceKeyRangeTest {

    private static SortedRanges of(final long... keys) {
        SortedRanges sr = SortedRanges.makeSingleElement(keys[0]);
        for (int i = 1; i < keys.length; ++i) {
            sr = sr.add(keys[i]);
            assertTrue("shape too large for a SortedRanges", sr != null);
        }
        return sr;
    }

    private static SortedRanges rangeThen(final long first, final long last, final long... keys) {
        SortedRanges sr = SortedRanges.makeSingleRange(first, last);
        for (final long k : keys) {
            sr = sr.add(k);
            assertTrue("shape too large for a SortedRanges", sr != null);
        }
        return sr;
    }

    private static void assertEmptyKeyRange(final String m, final SortedRanges sr, final long start, final long end) {
        try (final RowSequence rs = sr.getRowSequenceByKeyRange(start, end)) {
            assertEquals(m + ": size", 0, rs.size());
            assertTrue(m + ": isEmpty", rs.isEmpty());
            assertEquals(m + ": firstRowKey", RowSequence.NULL_ROW_KEY, rs.firstRowKey());
            assertEquals(m + ": lastRowKey", RowSequence.NULL_ROW_KEY, rs.lastRowKey());
        }
        // The same query through the public RowSet API.
        try (final RowSet rowSet = new WritableRowSetImpl(sr.ixCowRef());
                final RowSequence rs = rowSet.getRowSequenceByKeyRange(start, end)) {
            assertEquals(m + " [via RowSet]: size", 0, rs.size());
            assertTrue(m + " [via RowSet]: isEmpty", rs.isEmpty());
        }
    }

    /**
     * A key range whose end precedes its start selects nothing. {@link RspBitmap} and {@link SingleRange} both return
     * the empty sequence for such a range; SortedRanges must agree.
     */
    @Test
    public void testInvertedKeyRangeIsEmpty() {
        // Start key present as a singleton, with a following singleton.
        assertEmptyKeyRange("{5,7} (7,6)", of(5, 7), 7, 6);
        // Start key present as a singleton, with two following elements: reaches the index-vs-value comparison in
        // getRowSequenceByKeyRangePackedWithStart.
        assertEmptyKeyRange("{5,7,9} (7,6)", of(5, 7, 9), 7, 6);
        // Start key is the last element.
        assertEmptyKeyRange("{5,7,9} (9,6)", of(5, 7, 9), 9, 6);
        // Start key present, preceded by a range.
        assertEmptyKeyRange("{5-8,20} (20,19)", rangeThen(5, 8, 20), 20, 19);
        // Both bounds inside a range: this shape produced a negative-size sequence.
        assertEmptyKeyRange("{5-8,20} (8,6)", rangeThen(5, 8, 20), 8, 6);
        assertEmptyKeyRange("{5-8,20} (7,6)", rangeThen(5, 8, 20), 7, 6);
        // Wider shapes.
        assertEmptyKeyRange("{1,5-9,20-30,40} (25,24)", rangeThen(5, 9, 1, 20, 21, 22, 40), 25, 24);
        assertEmptyKeyRange("{1,5-9,20-30,40} (40,2)", rangeThen(5, 9, 1, 20, 21, 22, 40), 40, 2);
    }

    /** The other implementations already return the empty sequence; keep them pinned as the reference. */
    @Test
    public void testInvertedKeyRangeIsEmptyForOtherImplementations() {
        final RspBitmap rb = RspBitmap.makeEmpty().addRange(5, 8).add(20);
        rb.finishMutationsAndOptimize();
        try (final RowSequence rs = rb.ixGetRowSequenceByKeyRange(8, 6)) {
            assertTrue("RspBitmap", rs.isEmpty());
        }
        final OrderedLongSet single = SingleRange.make(5, 8);
        try (final RowSequence rs = single.ixGetRowSequenceByKeyRange(8, 6)) {
            assertTrue("SingleRange", rs.isEmpty());
        }
    }

    /**
     * A query for exactly the first key, when that key packs to 0 and is a lone singleton, is the only valid (start
     * &lt;= end) input for which {@code getRowSequenceByKeyRangePackedWithStart} takes its "next element is beyond our
     * end" shortcut. Every other valid input reaches the same result through the general loop.
     */
    @Test
    public void testQueryForFirstKeyPackedToZero() {
        SortedRanges sr = SortedRanges.makeSingleElement(0);
        sr = sr.add(5);
        try (final RowSequence rs = sr.getRowSequenceByKeyRange(0, 0)) {
            assertEquals(1, rs.size());
            assertEquals(0, rs.firstRowKey());
            assertEquals(0, rs.lastRowKey());
        }
        // The same shape with a range following the leading singleton.
        SortedRanges sr2 = SortedRanges.makeSingleElement(0);
        sr2 = sr2.addRange(5, 9);
        try (final RowSequence rs = sr2.getRowSequenceByKeyRange(0, 0)) {
            assertEquals(1, rs.size());
            assertEquals(0, rs.firstRowKey());
        }
    }

    /**
     * Exhaustively compare {@code getRowSequenceByKeyRange} against a model over every valid key range of a few shapes,
     * so that the branch selection inside {@code getRowSequenceByKeyRangePackedWithStart} is pinned.
     */
    @Test
    public void testAllValidKeyRangesMatchModel() {
        final SortedRanges[] shapes = new SortedRanges[] {
                of(5, 7),
                of(5, 7, 9),
                rangeThen(5, 8, 20),
                rangeThen(5, 9, 1, 20, 21, 22, 40),
                rangeThen(2, 3, 6, 8, 9, 10, 15),
        };
        for (final SortedRanges sr : shapes) {
            final TreeSet<Long> model = new TreeSet<>();
            sr.forEachLong(k -> {
                model.add(k);
                return true;
            });
            final long lo = model.first() - 2;
            final long hi = model.last() + 2;
            for (long start = lo; start <= hi; ++start) {
                for (long end = start; end <= hi; ++end) {
                    final List<Long> expected = new ArrayList<>(model.subSet(start, end + 1));
                    final List<Long> actual = new ArrayList<>();
                    try (final RowSequence rs = sr.getRowSequenceByKeyRange(start, end)) {
                        rs.forAllRowKeys(actual::add);
                        final String m = sr + " byKeyRange(" + start + "," + end + ")";
                        assertEquals(m, expected, actual);
                        assertEquals(m + ": size", expected.size(), (int) rs.size());
                        if (!expected.isEmpty()) {
                            assertEquals(m + ": first", expected.get(0).longValue(), rs.firstRowKey());
                            assertEquals(m + ": last", expected.get(expected.size() - 1).longValue(),
                                    rs.lastRowKey());
                        }
                    }
                }
            }
        }
    }
}
