//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link RowSequence} behavior that must not depend on which implementation happens to back the rowset. Each case is
 * run against all three, since a rowset built the ordinary way lands on one or another according to how many ranges it
 * holds, and a caller cannot tell which it got.
 */
public class RowSequenceContractTest {

    private static final String[] NAMES = {"single range", "sorted ranges", "rsp"};

    /** The single range [start, start + 4], backed by the implementation named at {@code which}. */
    private static WritableRowSet backing(final int which, final long start) {
        final long end = start + 4;
        switch (which) {
            case 0:
                return new WritableRowSetImpl(SingleRange.make(start, end));
            case 1:
                return new WritableRowSetImpl(SortedRanges.makeSingleRange(start, end));
            default:
                return new WritableRowSetImpl(RspBitmap.makeSingleRange(start, end));
        }
    }

    /**
     * Asking for no keys yields an empty sequence, wherever in the rowset the request starts.
     *
     * <p>
     * The fixture stays a single range on purpose: inserting a second, disjoint range would move the SingleRange case
     * onto another implementation, and its own guard would go untested.
     */
    @Test
    public void testNonPositiveLengthIsEmpty() {
        for (int i = 0; i < NAMES.length; ++i) {
            try (final WritableRowSet rs = backing(i, 5)) {
                assertBackedBy(NAMES[i], rs);
                for (final long length : new long[] {0, -1, -3}) {
                    for (final long pos : new long[] {0, 2, rs.size() - 1}) {
                        assertEmptyAt(NAMES[i], rs, pos, length);
                    }
                }
            }
        }
    }

    /**
     * The same, at a position inside a later range. SingleRange is absent by definition -- it cannot hold two disjoint
     * ranges -- so this covers only the implementations that can.
     */
    @Test
    public void testNonPositiveLengthIsEmptyAcrossSeveralRanges() {
        for (int i = 1; i < NAMES.length; ++i) {
            try (final WritableRowSet rs = backing(i, 5)) {
                rs.insertRange(100, 104);
                for (final long length : new long[] {0, -1}) {
                    for (final long pos : new long[] {0, 4, 6, rs.size() - 1}) {
                        assertEmptyAt(NAMES[i] + ", several ranges", rs, pos, length);
                    }
                }
            }
        }
    }

    /** Guards the fixture itself: a case meant to exercise one implementation must still be on it. */
    private static void assertBackedBy(final String what, final WritableRowSet rs) {
        final String backing = ((WritableRowSetImpl) rs).getInnerSet().getClass().getSimpleName();
        final String expected = what.equals("single range") ? "SingleRange"
                : what.equals("sorted ranges") ? "SortedRanges" : "Rsp";
        assertTrue(what + " is backed by " + backing, backing.contains(expected));
    }

    private static void assertEmptyAt(final String what, final WritableRowSet rs, final long pos, final long length) {
        final String where = what + " at position " + pos + " for length " + length;
        try (final RowSequence seq = rs.getRowSequenceByPosition(pos, length)) {
            assertEmpty(where, seq);
        }
        // A row sequence carries its own copy of the same guard, which a caller reaches by slicing again.
        try (final RowSequence whole = rs.getRowSequenceByPosition(0, rs.size());
                final RowSequence seq = whole.getRowSequenceByPosition(pos, length)) {
            assertEmpty(where + ", sliced again", seq);
        }
    }

    private static void assertEmpty(final String where, final RowSequence seq) {
        assertEquals(where + ": size", 0, seq.size());
        assertTrue(where + ": isEmpty", seq.isEmpty());
        assertTrue(where + ": no keys", seq.forEachRowKey(k -> {
            throw new AssertionError(where + ": produced key " + k);
        }));
    }

    /**
     * Filling a range chunk goes by its capacity. The size it arrives with says nothing about how much room there is,
     * and a caller who has already zeroed it is still owed the ranges.
     */
    @Test
    public void testFillRangesChunkIgnoresTheChunksIncomingSize() {
        for (int i = 0; i < NAMES.length; ++i) {
            final String what = NAMES[i];
            try (final WritableRowSet rs = backing(i, 5);
                    final RowSequence seq = rs.getRowSequenceByPosition(0, rs.size());
                    final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(16)) {
                chunk.setSize(0);
                seq.fillRowKeyRangesChunk(chunk);
                assertEquals(what + ": ranges filled", 2, chunk.size());
                assertEquals(what + ": start", rs.firstRowKey(), chunk.get(0));
                assertEquals(what + ": end", rs.lastRowKey(), chunk.get(1));
            }
        }
    }

    /** The same fill into a slice of a larger chunk, which is how a zero-sized chunk arrives in practice. */
    @Test
    public void testFillRangesChunkIntoASlice() {
        try (final WritableRowSet rs = backing(0, 5);
                final WritableLongChunk<OrderedRowKeyRanges> backing = WritableLongChunk.makeWritableChunk(16)) {
            backing.fillWithValue(0, 16, -1);
            final WritableLongChunk<OrderedRowKeyRanges> slice = backing.slice(4, 8);
            slice.setSize(0);
            try (final RowSequence seq = rs.getRowSequenceByPosition(0, rs.size())) {
                seq.fillRowKeyRangesChunk(slice);
            }
            assertEquals("ranges filled", 2, slice.size());
            assertEquals("start", 5, slice.get(0));
            assertEquals("end", 9, slice.get(1));
            assertEquals("did not write before the slice", -1, backing.get(3));
        }
    }
}
