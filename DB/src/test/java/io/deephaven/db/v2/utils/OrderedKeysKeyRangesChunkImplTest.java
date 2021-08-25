/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import org.junit.Test;

import static org.junit.Assert.*;

public class OrderedKeysKeyRangesChunkImplTest extends OrderedKeysTestBase {

    @Override
    protected OrderedKeys create(long... values) {
        return OrderedKeys.takeKeyRangesChunkAndMakeOrderedKeys(
                ChunkUtils.convertToOrderedKeyRanges(LongChunk.chunkWrap(values)));
    }

    @Test
    public void testGetRelativePositionForCoverage() {
        try (final WritableLongChunk<OrderedKeyRanges> chunk = WritableLongChunk.makeWritableChunk(6)) {
            chunk.setSize(0);
            chunk.add(0);
            chunk.add(3);
            chunk.add(10);
            chunk.add(13);
            chunk.add(20);
            chunk.add(23);
            final OrderedKeys ok = OrderedKeys.wrapKeyRangesChunkAsOrderedKeys(chunk);
            final OrderedKeys subOk = ok.getOrderedKeysByKeyRange(2, 22);
            final OrderedKeys.Iterator it = subOk.getOrderedKeysIterator();
            while (it.hasMore()) {
                final long pos0 = it.getRelativePosition();
                final OrderedKeys sit = it.getNextOrderedKeysWithLength(3);
                final long pos1 = it.getRelativePosition();
                assertEquals(sit.size(), pos1 - pos0);
            }
        }
    }
}
