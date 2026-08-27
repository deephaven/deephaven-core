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

/**
 * {@link RowSequence#fillRowKeyRangesChunk} goes by the chunk's capacity, not by whatever size it happens to arrive
 * with; a chunk handed over with size zero still gets filled.
 */
public class RowSequenceFillRangesChunkSizeTest {

    private static void assertFilledFromAnEmptySizedChunk(final String what, final RowSet rs) {
        try (final RowSequence seq = rs.getRowSequenceByPosition(0, rs.size());
                final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(16)) {
            chunk.setSize(0);
            seq.fillRowKeyRangesChunk(chunk);
            assertEquals(what + ": ranges filled", 2, chunk.size());
            assertEquals(what + ": start", rs.firstRowKey(), chunk.get(0));
            assertEquals(what + ": end", rs.lastRowKey(), chunk.get(1));
        }
    }

    @Test
    public void testFillRangesChunkIgnoresIncomingSize() {
        try (final RowSet single = new WritableRowSetImpl(SingleRange.make(5, 9));
                final RowSet sorted = new WritableRowSetImpl(SortedRanges.makeSingleRange(5, 9));
                final RowSet rsp = new WritableRowSetImpl(RspBitmap.makeSingleRange(5, 9))) {
            assertFilledFromAnEmptySizedChunk("sorted ranges", sorted);
            assertFilledFromAnEmptySizedChunk("rsp", rsp);
            assertFilledFromAnEmptySizedChunk("single range", single);
        }
    }

    @Test
    public void testFillRangesChunkIntoASlice() {
        try (final RowSet single = new WritableRowSetImpl(SingleRange.make(5, 9));
                final WritableLongChunk<OrderedRowKeyRanges> backing = WritableLongChunk.makeWritableChunk(16)) {
            backing.fillWithValue(0, 16, -1);
            final WritableLongChunk<OrderedRowKeyRanges> slice = backing.slice(4, 8);
            slice.setSize(0);
            try (final RowSequence seq = single.getRowSequenceByPosition(0, single.size())) {
                seq.fillRowKeyRangesChunk(slice);
            }
            assertEquals("ranges filled", 2, slice.size());
            assertEquals("start", 5, slice.get(0));
            assertEquals("end", 9, slice.get(1));
            assertEquals("did not write before the slice", -1, backing.get(3));
        }
    }
}
