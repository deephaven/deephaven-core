/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.FloatBuffer;

class FloatTransfer extends PrimitiveTransfer<WritableFloatChunk<Values>, FloatBuffer> {

    public static FloatTransfer create(@NotNull final ColumnSource<?> columnSource, final int targetSize) {
        final float[] backingArray = new float[targetSize];
        return new FloatTransfer(
                columnSource,
                WritableFloatChunk.writableChunkWrap(backingArray),
                FloatBuffer.wrap(backingArray),
                targetSize);
    }

    private FloatTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final WritableFloatChunk<Values> chunk,
            @NotNull final FloatBuffer buffer,
            final int targetSize) {
        super(columnSource, chunk, buffer, targetSize);
    }
}
