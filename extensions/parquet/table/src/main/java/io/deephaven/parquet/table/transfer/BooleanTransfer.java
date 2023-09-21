/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

class BooleanTransfer extends PrimitiveTransfer<WritableByteChunk<Values>, ByteBuffer> {

    public static BooleanTransfer create(@NotNull final ColumnSource<?> columnSource, int targetSize) {
        final byte[] backingArray = new byte[targetSize];
        return new BooleanTransfer(
                columnSource,
                WritableByteChunk.writableChunkWrap(backingArray),
                ByteBuffer.wrap(backingArray),
                targetSize);
    }

    private BooleanTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final WritableByteChunk<Values> chunk,
            @NotNull final ByteBuffer buffer,
            int targetSize) {
        super(columnSource, chunk, buffer, targetSize);
    }
}
