//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.by.PartitionByChunkedOperator;
import org.jetbrains.annotations.NotNull;

/**
 * This is a niche class that allows an existing {@link WritableRowSet rowset} to masquerade as a
 * {@link RowSetBuilderRandom} for the sake of efficient processing.
 *
 * General usage would embed this in a collection with actual {@link RowSetBuilderRandom} and process all items
 * similarly. See
 * {@link PartitionByChunkedOperator#appendShifts(io.deephaven.chunk.LongChunk, io.deephaven.chunk.LongChunk, int, int, long)}
 * and {@link PartitionByChunkedOperator#extractAndClearBuilderRandom(io.deephaven.chunk.WritableObjectChunk, int)} for
 * an example.
 */

public class PrecomputedRowSetBuilderRandom implements RowSetBuilderRandom {
    WritableRowSet rowSet;

    private PrecomputedRowSetBuilderRandom(@NotNull final WritableRowSet rowSet) {
        this.rowSet = rowSet;
    }

    @Override
    public WritableRowSet build() {
        if (rowSet == null) {
            throw new IllegalStateException("PrecomputedRowSetBuilderRandom - build() already called)");
        }
        // make sure to release our reference to the rowset after `build()` is called
        WritableRowSet tmp = rowSet;
        rowSet = null;
        return tmp;
    }

    @Override
    public void addKey(long rowKey) {
        throw new UnsupportedOperationException("PrecomputedRowSetBuilderRandom does not allow modifications)");
    }

    @Override
    public void addRange(long firstRowKey, long lastRowKey) {
        throw new UnsupportedOperationException("PrecomputedRowSetBuilderRandom does not allow modifications");
    }

    public static PrecomputedRowSetBuilderRandom createFromRowSet(@NotNull final WritableRowSet rowSet) {
        return new PrecomputedRowSetBuilderRandom(rowSet);
    }
}
