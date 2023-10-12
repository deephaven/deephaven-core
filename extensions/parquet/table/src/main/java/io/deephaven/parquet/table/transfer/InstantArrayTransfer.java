/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.Instant;

final class InstantArrayTransfer extends PrimitiveArrayAndVectorTransfer<Instant[], Instant[], LongBuffer> {
    // We encode Instants as primitive longs
    InstantArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Long.BYTES, targetPageSize,
                LongBuffer.allocate(targetPageSize / Long.BYTES), Long.BYTES);
    }

    @Override
    int getSize(final Instant @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final @NotNull EncodedData<Instant[]> data) {
        for (Instant t : data.encodedValues) {
            buffer.put(DateTimeUtils.epochNanos(t));
        }
    }
}
