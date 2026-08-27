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

    /** Asking for zero keys yields an empty sequence, wherever in the rowset the request starts. */
    @Test
    public void testZeroLengthIsEmpty() {
        for (int i = 0; i < NAMES.length; ++i) {
            final String what = NAMES[i];
            try (final WritableRowSet rs = backing(i, 5)) {
                rs.insertRange(100, 104);
                for (final long pos : new long[] {0, 2, rs.size() - 1}) {
                    try (final RowSequence seq = rs.getRowSequenceByPosition(pos, 0)) {
                        assertEquals(what + ": size at " + pos, 0, seq.size());
                        assertTrue(what + ": isEmpty at " + pos, seq.isEmpty());
                        assertTrue(what + ": no keys at " + pos, seq.forEachRowKey(k -> {
                            throw new AssertionError(what + ": produced key " + k);
                        }));
                    }
                }
                // A negative length asks for nothing at all, the same as zero.
                try (final RowSequence seq = rs.getRowSequenceByPosition(0, -1)) {
                    assertEquals(what + ": size for a negative length", 0, seq.size());
                }
            }
        }
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
