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
    DateArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES), Integer.BYTES);
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
    void copyToBuffer(final @NotNull EncodedData<LocalDate[]> data) {
        for (final LocalDate t : data.encodedValues) {
            buffer.put(DateTimeUtils.epochDays(t));
        }
    }
}
