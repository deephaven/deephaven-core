/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
import io.deephaven.engine.chunk.Attributes.OrderedRowKeyRanges;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import org.junit.Test;

import static org.junit.Assert.*;

public class RowSequenceKeyRangesChunkImplTest extends RowSequenceTestBase {

    @Override
    protected RowSequence create(long... values) {
        return RowSequenceUtil.takeKeyRangesChunkAndMakeRowSequence(
                ChunkUtils.convertToOrderedKeyRanges(LongChunk.chunkWrap(values)));
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
            final RowSequence rs = RowSequenceUtil.wrapKeyRangesChunkAsRowSequence(chunk);
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
}
