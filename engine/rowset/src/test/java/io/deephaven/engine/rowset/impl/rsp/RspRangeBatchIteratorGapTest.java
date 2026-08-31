//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static org.junit.Assert.assertEquals;

/**
 * Two full block spans are represented as one span only when they are adjacent; separated by an empty block they are
 * distinct spans. Filling range chunks from a {@link RowSequence} slice has to cope with meeting a second full block
 * span right after finishing the first.
 */
public class RspRangeBatchIteratorGapTest {

    private static final long BS = BLOCK_SIZE;

    /**
     * Blocks 2 and 3 full, block 4 empty, blocks 7 and 8 full: two full block spans with a gap between them. Built as
     * an {@link RspBitmap} on purpose -- two ranges would otherwise fit a SortedRanges, whose row sequences never reach
     * the RSP range iterators.
     */
    private static WritableRowSet twoFullBlockSpansWithAGap() {
        RspBitmap rb = RspBitmap.makeEmpty();
        rb = rb.appendRangeUnsafe(2 * BS, 4 * BS - 1);
        rb = rb.appendRangeUnsafe(7 * BS, 9 * BS - 1);
        rb.finishMutations();
        return new WritableRowSetImpl(rb);
    }

    private static List<long[]> chunkRangesOf(final RowSequence rowSequence) {
        final List<long[]> out = new ArrayList<>();
        try (final WritableLongChunk<OrderedRowKeyRanges> chunk =
                WritableLongChunk.makeWritableChunk(2 * (int) rowSequence.size() + 16)) {
            chunk.setSize(0);
            rowSequence.fillRowKeyRangesChunk(chunk);
            for (int i = 0; i < chunk.size(); i += 2) {
                out.add(new long[] {chunk.get(i), chunk.get(i + 1)});
            }
        }
        return out;
    }

    private static List<long[]> asChunkRangesOf(final RowSequence rowSequence) {
        final List<long[]> out = new ArrayList<>();
        final io.deephaven.chunk.LongChunk<OrderedRowKeyRanges> chunk = rowSequence.asRowKeyRangesChunk();
        for (int i = 0; i < chunk.size(); i += 2) {
            out.add(new long[] {chunk.get(i), chunk.get(i + 1)});
        }
        return out;
    }

    @Test
    public void testAsRangesChunkFromWholeSliceAcrossTheGap() {
        try (final WritableRowSet rs = twoFullBlockSpansWithAGap();
                final RowSequence slice = rs.getRowSequenceByPosition(0, rs.size())) {
            assertEquals(render(rangesOf(rs)), render(asChunkRangesOf(slice)));
        }
    }

    @Test
    public void testAsRangesChunkFromAKeyRangeSliceAcrossTheGap() {
        try (final WritableRowSet rs = twoFullBlockSpansWithAGap();
                final RowSequence slice = rs.getRowSequenceByKeyRange(2 * BS + 5, 9 * BS - 3);
                final WritableRowSet expected = rs.subSetByKeyRange(2 * BS + 5, 9 * BS - 3)) {
            assertEquals(render(rangesOf(expected)), render(asChunkRangesOf(slice)));
        }
    }

    @Test
    public void testFillRangesFromWholeSliceAcrossTheGap() {
        try (final WritableRowSet rs = twoFullBlockSpansWithAGap();
                final RowSequence slice = rs.getRowSequenceByPosition(0, rs.size())) {
            assertEquals(render(rangesOf(rs)), render(chunkRangesOf(slice)));
        }
    }

    @Test
    public void testAsRangesChunkFromIteratorSlices() {
        try (final WritableRowSet rs = twoFullBlockSpansWithAGap();
                final RowSequence.Iterator it = rs.getRowSequenceIterator()) {
            final List<long[]> got = new ArrayList<>();
            while (it.hasMore()) {
                final RowSequence slice = it.getNextRowSequenceWithLength(3 * BS);
                for (final long[] r : chunkRangesOf(slice)) {
                    got.add(r);
                }
            }
            // Slices may split ranges at their boundaries, so compare the keys the ranges cover.
            long count = 0;
            for (final long[] r : got) {
                count += r[1] - r[0] + 1;
            }
            assertEquals("every key must be covered exactly once", rs.size(), count);
            long prevEnd = -2;
            for (final long[] r : got) {
                if (r[0] > prevEnd + 1) {
                    // a genuine gap; fine
                } else {
                    assertEquals("ranges must not overlap", prevEnd + 1, r[0]);
                }
                prevEnd = r[1];
            }
        }
    }

    @Test
    public void testFillRangesFromAKeyRangeSliceAcrossTheGap() {
        try (final WritableRowSet rs = twoFullBlockSpansWithAGap();
                final RowSequence slice = rs.getRowSequenceByKeyRange(2 * BS + 5, 9 * BS - 3)) {
            try (final WritableRowSet expected = rs.subSetByKeyRange(2 * BS + 5, 9 * BS - 3)) {
                assertEquals(render(rangesOf(expected)), render(chunkRangesOf(slice)));
            }
        }
    }
}
