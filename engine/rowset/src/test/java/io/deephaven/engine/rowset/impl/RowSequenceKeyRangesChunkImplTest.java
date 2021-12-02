/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

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
}
