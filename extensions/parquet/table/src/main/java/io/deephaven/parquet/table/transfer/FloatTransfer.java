//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.FloatBuffer;

final class FloatTransfer extends FillingPrimitiveTransfer<WritableFloatChunk<Values>, FloatBuffer> {
    static FloatTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
            final int targetPageSizeInBytes) {
        final int targetElementsPerPage =
                Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Float.BYTES));
        final float[] backingArray = new float[targetElementsPerPage];
        return new FloatTransfer(
                columnSource,
                tableRowSet,
                WritableFloatChunk.writableChunkWrap(backingArray),
                FloatBuffer.wrap(backingArray),
                targetElementsPerPage);
    }

    private FloatTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableFloatChunk<Values> chunk,
            @NotNull final FloatBuffer buffer,
            final int targetElementsPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, targetElementsPerPage);
    }
}
