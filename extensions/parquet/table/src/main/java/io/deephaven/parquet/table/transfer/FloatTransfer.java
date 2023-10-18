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
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.FloatBuffer;

final class FloatTransfer extends PrimitiveTransfer<WritableFloatChunk<Values>, FloatBuffer> {
    static FloatTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
                              final int targetPageSize) {
        final int maxValuesPerPage = Math.toIntExact(Math.min(tableRowSet.size(), targetPageSize / Float.BYTES));
        final float[] backingArray = new float[maxValuesPerPage];
        return new FloatTransfer(
                columnSource,
                tableRowSet,
                WritableFloatChunk.writableChunkWrap(backingArray),
                FloatBuffer.wrap(backingArray),
                maxValuesPerPage);
    }

    private FloatTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableFloatChunk<Values> chunk,
            @NotNull final FloatBuffer buffer,
            final int maxValuesPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, maxValuesPerPage);
    }
}
