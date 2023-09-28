/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.LongVector;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;

final class LongVectorTransfer extends PrimitiveVectorTransfer<LongVector, LongBuffer> {
    LongVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Long.BYTES, targetPageSize,
                LongBuffer.allocate(targetPageSize / Long.BYTES), Long.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final LongVector data) {
        data.iterator().forEachRemaining((long value) -> buffer.put(value));
    }
}