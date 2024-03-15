//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit InstantArrayTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.LocalTime;

final class TimeArrayTransfer extends PrimitiveArrayAndVectorTransfer<LocalTime[], LocalTime[], LongBuffer> {
    // We encode LocalTime as primitive longs
    TimeArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Long.BYTES, targetPageSizeInBytes,
                LongBuffer.allocate(targetPageSizeInBytes / Long.BYTES), Long.BYTES);
    }

    @Override
    int getSize(final LocalTime @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<LocalTime[]> data) {
        for (final LocalTime t : data.encodedValues) {
            buffer.put(DateTimeUtils.nanosOfDay(t));
        }
    }
}
