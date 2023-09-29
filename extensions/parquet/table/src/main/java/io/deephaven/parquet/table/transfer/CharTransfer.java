/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

class CharTransfer extends IntCastablePrimitiveTransfer<CharChunk<Values>> {

    public CharTransfer(@NotNull final ColumnSource<?> columnSource, final int targetSize) {
        super(columnSource, targetSize);
    }

    @Override
    public void copyAllFromChunkToBuffer() {
        for (int chunkIdx = 0; chunkIdx < chunk.size(); ++chunkIdx) {
            buffer.put(chunk.get(chunkIdx));
        }
    }
}
