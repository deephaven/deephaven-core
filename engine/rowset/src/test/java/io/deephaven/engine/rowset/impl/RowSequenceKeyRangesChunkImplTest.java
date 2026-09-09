//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequenceFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class RowSequenceKeyRangesChunkImplTest extends RowSequenceTestBase {

    @Override
    protected RowSequence create(long... values) {
        return RowSequenceFactory.takeKeyRangesChunkAndMakeRowSequence(
                RowKeyChunkUtils.convertToOrderedKeyRanges(LongChunk.chunkWrap(values)));
    }

    @Test
    public void testGetRelativePositionForCoverage() {
        try (final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(6)) {
            chunk.setSize(0);
            chunk.add(0);
            chunk.add(3);
            chunk.add(10);
            chunk.add(13);
            chunk.add(20);
            chunk.add(23);
            final RowSequence rs = RowSequenceFactory.wrapKeyRangesChunkAsRowSequence(chunk);
            final RowSequence subOk = rs.getRowSequenceByKeyRange(2, 22);
            final RowSequence.Iterator it = subOk.getRowSequenceIterator();
            while (it.hasMore()) {
                final long pos0 = it.getRelativePosition();
                final RowSequence sit = it.getNextRowSequenceWithLength(3);
                final long pos1 = it.getRelativePosition();
                assertEquals(sit.size(), pos1 - pos0);
            }
        }
    }

    @Test
    public void testGetNextThroughConsumedRegionIsEmpty() {
        try (final RowSequence rs = RowSequenceFactory
                .wrapKeyRangesChunkAsRowSequence(LongChunk.chunkWrap(new long[] {0, 5}))) {
            try (final RowSequence.Iterator it = rs.getRowSequenceIterator()) {
                final RowSequence first = it.getNextRowSequenceWithLength(3); // consumes 0..2
                assertEquals(3, first.size());
                // A max key inside the already-consumed part of the current range must yield EMPTY,
                // not a corrupt (min > max) slice.
                final RowSequence empty = it.getNextRowSequenceThrough(1);
                assertTrue(empty.isEmpty());
                assertEquals(0, empty.size());
                assertEquals(3, it.peekNextKey());
            }
        }
    }

    @Test
    public void testAdvanceHonorsMaxKey() {
        try (final RowSequence rs = RowSequenceFactory
                .wrapKeyRangesChunkAsRowSequence(LongChunk.chunkWrap(new long[] {10, 15}))) {
            try (final RowSequence sub = rs.getRowSequenceByKeyRange(10, 12)) {
                try (final RowSequence.Iterator it = sub.getRowSequenceIterator()) {
                    // Advancing past the sub-sequence's max key exhausts it, even though the backing
                    // chunk has more keys.
                    assertFalse(it.advance(14));
                    assertFalse(it.hasMore());
                }
            }
        }
    }

    @Test
    public void testFillRowKeyRangesChunkOnEmptySequence() {
        try (final RowSequence rs = RowSequenceFactory
                .wrapKeyRangesChunkAsRowSequence(LongChunk.chunkWrap(new long[0]))) {
            try (final WritableLongChunk<OrderedRowKeyRanges> chunk = WritableLongChunk.makeWritableChunk(4)) {
                rs.fillRowKeyRangesChunk(chunk);
                assertEquals(0, chunk.size());
            }
        }
    }
}
