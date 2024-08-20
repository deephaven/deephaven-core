//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntVectorTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.LongVector;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;

final class LongVectorTransfer extends PrimitiveVectorTransfer<LongVector, LongBuffer> {
    LongVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Long.BYTES, targetPageSizeInBytes,
                LongBuffer.allocate(targetPageSizeInBytes / Long.BYTES), Long.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<LongVector> data) {
        try (final CloseablePrimitiveIteratorOfLong dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((long value) -> buffer.put(value));
        }
    }
}
