//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TimeTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.parquet.base.ParquetTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.LocalDateTime;

final class LocalDateTimeTransfer extends GettingPrimitiveTransfer<ObjectChunk<LocalDateTime, Values>, LongBuffer> {

    LocalDateTimeTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet,
                LongBuffer.allocate(Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Long.BYTES))),
                Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Long.BYTES)));
    }

    @Override
    void copyAllFromChunkToBuffer() {
        final int chunkSize = chunk.size();
        for (int chunkIdx = 0; chunkIdx < chunkSize; ++chunkIdx) {
            buffer.put(ParquetTimeUtils.epochNanosUTC(chunk.get(chunkIdx)));
        }
    }
}
