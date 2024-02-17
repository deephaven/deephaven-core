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

import java.nio.LongBuffer;
import java.time.Instant;

final class InstantVectorTransfer extends PrimitiveVectorTransfer<ObjectVector<Instant>, LongBuffer> {
    // We encode Instant as primitive longs
    InstantVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Long.BYTES, targetPageSizeInBytes,
                LongBuffer.allocate(targetPageSizeInBytes / Long.BYTES), Long.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<ObjectVector<Instant>> data) {
        try (final CloseableIterator<Instant> dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((Instant t) -> buffer.put(DateTimeUtils.epochNanos(t)));
        }
    }
}
