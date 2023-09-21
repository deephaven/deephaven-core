/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;

class LongTransfer extends PrimitiveTransfer<WritableLongChunk<Values>, LongBuffer> {

    public static LongTransfer create(@NotNull final ColumnSource<?> columnSource, final int targetSize) {
        final long[] backingArray = new long[targetSize];
        return new LongTransfer(
                columnSource,
                WritableLongChunk.writableChunkWrap(backingArray),
                LongBuffer.wrap(backingArray),
                targetSize);
    }

    private LongTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final WritableLongChunk<Values> chunk,
            @NotNull final LongBuffer buffer,
            final int targetSize) {
        super(columnSource, chunk, buffer, targetSize);
    }
}
