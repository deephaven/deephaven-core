//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntVectorTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.FloatVector;
import org.jetbrains.annotations.NotNull;

import java.nio.FloatBuffer;

final class FloatVectorTransfer extends PrimitiveVectorTransfer<FloatVector, FloatBuffer> {
    FloatVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Float.BYTES, targetPageSizeInBytes,
                FloatBuffer.allocate(targetPageSizeInBytes / Float.BYTES), Float.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = FloatBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<FloatVector> data) {
        try (final CloseablePrimitiveIteratorOfFloat dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((float value) -> buffer.put(value));
        }
    }
}
