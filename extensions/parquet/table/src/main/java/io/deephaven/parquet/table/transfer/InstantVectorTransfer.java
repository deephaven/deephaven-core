/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.Instant;

final class InstantVectorTransfer extends PrimitiveVectorTransfer<ObjectVector<Instant>, LongBuffer> {
    InstantVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                LongBuffer.allocate(targetPageSize / Integer.BYTES), Integer.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final ObjectVector<Instant> data) {
        data.iterator().forEachRemaining((Instant value) -> buffer.put(DateTimeUtils.epochNanos(value)));
    }
}
