//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit InstantVectorTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
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
    // We encode LocalDate as primitive ints
    DateVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Integer.BYTES, targetPageSizeInBytes,
                IntBuffer.allocate(targetPageSizeInBytes / Integer.BYTES), Integer.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<ObjectVector<LocalDate>> data) {
        try (final CloseableIterator<LocalDate> dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((LocalDate t) -> buffer.put(DateTimeUtils.epochDaysAsInt(t)));
        }
    }
}
