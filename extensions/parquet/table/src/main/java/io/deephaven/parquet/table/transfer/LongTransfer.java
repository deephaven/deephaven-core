//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;

final class LongTransfer extends FillingPrimitiveTransfer<WritableLongChunk<Values>, LongBuffer> {
    static LongTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
            final int targetPageSizeInBytes) {
        final int targetElementsPerPage =
                Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Long.BYTES));
        final long[] backingArray = new long[targetElementsPerPage];
        return new LongTransfer(
                columnSource,
                tableRowSet,
                WritableLongChunk.writableChunkWrap(backingArray),
                LongBuffer.wrap(backingArray),
                targetElementsPerPage);
    }

    private LongTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableLongChunk<Values> chunk,
            @NotNull final LongBuffer buffer,
            final int targetElementsPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, targetElementsPerPage);
    }
}
