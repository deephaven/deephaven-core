//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;

/**
 * An {@link OrderedLongSet.BuilderSequential} that appends each key to a chunk instead of building a set.
 */
public final class RowKeysChunkBuilderSequential implements OrderedLongSet.BuilderSequential {
    private final WritableLongChunk<? super OrderedRowKeys> chunk;

    public RowKeysChunkBuilderSequential(final WritableLongChunk<? super OrderedRowKeys> chunk) {
        this.chunk = chunk;
        chunk.setSize(0);
    }

    @Override
    public OrderedLongSet getOrderedLongSet() {
        throw new UnsupportedOperationException("Keys are appended to a chunk");
    }

    @Override
    public void appendKey(final long key) {
        chunk.add(key);
    }

    @Override
    public void appendRange(final long firstKey, final long lastKey) {
        for (long key = firstKey; key <= lastKey; ++key) {
            chunk.add(key);
        }
    }
}
