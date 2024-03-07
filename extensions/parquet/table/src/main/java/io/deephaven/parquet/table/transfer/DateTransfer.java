//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;

final class DateTransfer extends IntCastablePrimitiveTransfer<ObjectChunk<LocalDate, Values>> {
    DateTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet, final int targetSize) {
        super(columnSource, tableRowSet, targetSize);
    }

    @Override
    public void copyAllFromChunkToBuffer() {
        final int chunkSize = chunk.size();
        for (int chunkIdx = 0; chunkIdx < chunkSize; ++chunkIdx) {
            // Store the number of days from the Unix epoch, 1 January 1970
            buffer.put(DateTimeUtils.epochDaysAsInt(chunk.get(chunkIdx)));
        }
    }
}
