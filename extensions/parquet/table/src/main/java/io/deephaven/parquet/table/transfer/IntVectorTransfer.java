/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.IntVector;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class IntVectorTransfer extends PrimitiveVectorTransfer<IntVector, IntBuffer> {
    IntVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES), Integer.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<IntVector> data) {
        try (final CloseablePrimitiveIteratorOfInt dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((int value) -> buffer.put(value));
        }
    }
}