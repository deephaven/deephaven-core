//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntVectorTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfDouble;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.DoubleVector;
import org.jetbrains.annotations.NotNull;

import java.nio.DoubleBuffer;

final class DoubleVectorTransfer extends PrimitiveVectorTransfer<DoubleVector, DoubleBuffer> {
    DoubleVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Double.BYTES, targetPageSizeInBytes,
                DoubleBuffer.allocate(targetPageSizeInBytes / Double.BYTES), Double.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = DoubleBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<DoubleVector> data) {
        try (final CloseablePrimitiveIteratorOfDouble dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((double value) -> buffer.put(value));
        }
    }
}
