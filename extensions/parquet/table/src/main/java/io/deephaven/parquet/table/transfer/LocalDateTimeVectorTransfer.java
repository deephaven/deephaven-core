/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit InstantVectorTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.LocalDateTime;

final class LocalDateTimeVectorTransfer extends PrimitiveVectorTransfer<ObjectVector<LocalDateTime>, LongBuffer> {
    // We encode LocalDateTime as primitive longs
    LocalDateTimeVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Long.BYTES, targetPageSizeInBytes,
                LongBuffer.allocate(targetPageSizeInBytes / Long.BYTES), Long.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<ObjectVector<LocalDateTime>> data) {
        try (final CloseableIterator<LocalDateTime> dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((LocalDateTime t) -> buffer.put(DateTimeUtils.epochNanosUTC(t)));
        }
    }
}
