/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;
import java.time.LocalDate;

final class DateArrayTransfer extends PrimitiveArrayAndVectorTransfer<LocalDate[], LocalDate[], IntBuffer> {
    private static int getMaxValuesPerPage(@NotNull final RowSequence tableRowSet, final int targetPageSize) {
        return Math.toIntExact(Math.min(tableRowSet.size(), targetPageSize / Integer.BYTES));
    }

    DateArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, getMaxValuesPerPage(tableRowSet, targetPageSize), targetPageSize,
                IntBuffer.allocate(getMaxValuesPerPage(tableRowSet, targetPageSize)), Integer.BYTES);
    }

    @Override
    int getSize(final LocalDate @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<LocalDate[]> data) {
        for (final LocalDate t : data.encodedValues) {
            // Store the number of days from the Unix epoch, 1 January 1970
            buffer.put(DateTimeUtils.epochDaysAsInt(t));
        }
    }
}
