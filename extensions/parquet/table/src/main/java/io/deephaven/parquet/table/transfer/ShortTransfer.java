/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

class ShortTransfer extends IntCastablePrimitiveTransfer<ShortChunk<Values>> {

    public ShortTransfer(@NotNull final ColumnSource<?> columnSource, final int targetSize) {
        super(columnSource, targetSize);
    }

    @Override
    public void copyAllFromChunkToBuffer() {
        for (int chunkIdx = 0; chunkIdx < chunk.size(); ++chunkIdx) {
            buffer.put(chunk.get(chunkIdx));
        }
    }
}
