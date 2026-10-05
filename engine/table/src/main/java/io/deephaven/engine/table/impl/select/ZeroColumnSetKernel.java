//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.tuple.EmptyTuple;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SetKernel} for a key of no columns, whose one key, the empty tuple, is in the set while any set row holds
 * it; every row then matches.
 */
final class ZeroColumnSetKernel extends SetKernel {

    /** The number of set rows. */
    private long rows;
    /** The number of set rows when the current update began. */
    private long rowsAtUpdateStart;

    @Override
    long size() {
        return rows > 0 ? 1 : 0;
    }

    @Override
    void beginUpdate() {
        rowsAtUpdateStart = rows;
    }

    @Override
    void add(@NotNull final RowSequence rows) {
        this.rows += rows.size();
    }

    @Override
    void remove(@NotNull final RowSequence rows) {
        this.rows -= rows.size();
        Assert.geqZero(this.rows, "this.rows");
    }

    @Override
    void finishRemove(@NotNull final RowSequence removedRows) {}

    @Override
    boolean keysAdded() {
        return rowsAtUpdateStart == 0 && rows > 0;
    }

    @Override
    boolean keysRemoved() {
        return rowsAtUpdateStart > 0 && rows == 0;
    }

    @Override
    void matchValues(
            @NotNull final MatchContext context,
            @NotNull final Chunk<Values>[] keyChunks,
            @NotNull final LongChunk<OrderedRowKeys> rowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusion) {
        if ((rows > 0) == inclusion) {
            results.copyFromChunk(rowKeys, 0, 0, rowKeys.size());
            results.setSize(rowKeys.size());
        } else {
            results.setSize(0);
        }
    }

    @Override
    CloseableIterator<Object> iterator() {
        return rows > 0 ? CloseableIterator.of(EmptyTuple.INSTANCE) : CloseableIterator.empty();
    }
}
