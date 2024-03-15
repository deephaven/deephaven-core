//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.LocalTime;

final class TimeTransfer extends GettingPrimitiveTransfer<ObjectChunk<LocalTime, Values>, LongBuffer> {

    TimeTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet,
                LongBuffer.allocate(Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Long.BYTES))),
                Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Long.BYTES)));
    }

    @Override
    void copyAllFromChunkToBuffer() {
        final int chunkSize = chunk.size();
        for (int chunkIdx = 0; chunkIdx < chunkSize; ++chunkIdx) {
            buffer.put(DateTimeUtils.nanosOfDay(chunk.get(chunkIdx)));
        }
    }
}
