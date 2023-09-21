/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

class IntTransfer extends PrimitiveTransfer<WritableIntChunk<Values>, IntBuffer> {

    public static IntTransfer create(@NotNull final ColumnSource<?> columnSource, final int targetSize) {
        final int[] backingArray = new int[targetSize];
        return new IntTransfer(
                columnSource,
                WritableIntChunk.writableChunkWrap(backingArray),
                IntBuffer.wrap(backingArray),
                targetSize);
    }

    private IntTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final WritableIntChunk<Values> chunk,
            @NotNull final IntBuffer buffer,
            final int targetSize) {
        super(columnSource, chunk, buffer, targetSize);
    }
}
