/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.DoubleBuffer;

final class DoubleTransfer extends PrimitiveTransfer<WritableDoubleChunk<Values>, DoubleBuffer> {
    static DoubleTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
                              final int targetPageSize) {
        final int maxValuesPerPage = Math.toIntExact(Math.min(tableRowSet.size(), targetPageSize / Double.BYTES));
        final double[] backingArray = new double[maxValuesPerPage];
        return new DoubleTransfer(
                columnSource,
                tableRowSet,
                WritableDoubleChunk.writableChunkWrap(backingArray),
                DoubleBuffer.wrap(backingArray),
                maxValuesPerPage);
    }

    private DoubleTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableDoubleChunk<Values> chunk,
            @NotNull final DoubleBuffer buffer,
            final int maxValuesPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, maxValuesPerPage);
    }
}
