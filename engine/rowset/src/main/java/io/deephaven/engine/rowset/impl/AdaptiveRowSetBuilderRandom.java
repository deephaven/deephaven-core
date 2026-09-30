//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;

/**
 * {@link RowSetBuilderRandom} implementation that uses an adaptive pattern based on workload.
 */
public class AdaptiveRowSetBuilderRandom implements RowSetBuilderRandom {

    private final AdaptiveOrderedLongSetBuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();

    @Override
    public WritableRowSet build() {
        return new WritableRowSetImpl(builder.getOrderedLongSet());
    }

    @Override
    public void addKey(final long rowKey) {
        builder.addKey(rowKey);
    }

    @Override
    public void addRange(final long firstRowKey, final long lastRowKey) {
        builder.addRange(firstRowKey, lastRowKey);
    }

    @Override
    public void addOrderedRowKeysChunk(final LongChunk<? extends OrderedRowKeys> chunk) {
        builder.addOrderedRowKeysChunk(chunk, 0, chunk.size());
    }

    @Override
    public void addOrderedRowKeysChunk(final LongChunk<OrderedRowKeys> chunk, final int offset, final int length) {
        builder.addOrderedRowKeysChunk(chunk, offset, length);
    }

    @Override
    public void addOrderedRowKeysChunk(final IntChunk<? extends OrderedRowKeys> chunk) {
        builder.addOrderedRowKeysChunk(chunk, 0, chunk.size());
    }

    @Override
    public void addRowSet(final RowSet rowSet) {
        // The inner builder can take the row set's implementation whole, rather than walking it range by range.
        builder.addRowSet(rowSet);
    }
}
