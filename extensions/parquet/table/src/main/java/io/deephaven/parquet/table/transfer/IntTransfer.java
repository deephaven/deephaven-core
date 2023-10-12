/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class IntTransfer extends PrimitiveTransfer<WritableIntChunk<Values>, IntBuffer> {
    static IntTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
                              final int targetPageSize) {
        final int maxValuesPerPage = Math.toIntExact(Math.min(tableRowSet.size(), targetPageSize / Integer.BYTES));
        final int[] backingArray = new int[maxValuesPerPage];
        return new IntTransfer(
                columnSource,
                tableRowSet,
                WritableIntChunk.writableChunkWrap(backingArray),
                IntBuffer.wrap(backingArray),
                maxValuesPerPage);
    }

    private IntTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableIntChunk<Values> chunk,
            @NotNull final IntBuffer buffer,
            final int maxValuesPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, maxValuesPerPage);
    }
}
