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

final class IntTransfer extends FillingPrimitiveTransfer<WritableIntChunk<Values>, IntBuffer> {
    static IntTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
                              final int targetPageSizeInBytes) {
        final int targetElementsPerPage = Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Integer.BYTES));
        final int[] backingArray = new int[targetElementsPerPage];
        return new IntTransfer(
                columnSource,
                tableRowSet,
                WritableIntChunk.writableChunkWrap(backingArray),
                IntBuffer.wrap(backingArray),
                targetElementsPerPage);
    }

    private IntTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableIntChunk<Values> chunk,
            @NotNull final IntBuffer buffer,
            final int targetElementsPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, targetElementsPerPage);
    }
}
