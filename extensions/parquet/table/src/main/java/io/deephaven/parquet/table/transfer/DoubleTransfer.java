/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.DoubleBuffer;

class DoubleTransfer extends PrimitiveTransfer<WritableDoubleChunk<Values>, DoubleBuffer> {

    public static DoubleTransfer create(@NotNull final ColumnSource<?> columnSource, final int targetSize) {
        final double[] backingArray = new double[targetSize];
        return new DoubleTransfer(
                columnSource,
                WritableDoubleChunk.writableChunkWrap(backingArray),
                DoubleBuffer.wrap(backingArray),
                targetSize);
    }

    private DoubleTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final WritableDoubleChunk<Values> chunk,
            @NotNull final DoubleBuffer buffer,
            final int targetSize) {
        super(columnSource, chunk, buffer, targetSize);
    }
}
