/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;
import java.time.LocalDate;

final class DateVectorTransfer extends PrimitiveVectorTransfer<ObjectVector<LocalDate>, IntBuffer> {

    DateVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES), Integer.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<ObjectVector<LocalDate>> data) {
        try (final CloseableIterator<LocalDate> dataIterator = data.encodedValues.iterator()) {
            // Store the number of days from the Unix epoch, 1 January 1970
            dataIterator.forEachRemaining((LocalDate t) -> buffer.put(DateTimeUtils.epochDaysAsInt(t)));
        }
    }
}
