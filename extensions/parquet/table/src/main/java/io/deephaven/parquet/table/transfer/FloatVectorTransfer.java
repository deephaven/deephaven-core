/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntVectorTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfFloat;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.FloatVector;
import org.jetbrains.annotations.NotNull;

import java.nio.FloatBuffer;

final class FloatVectorTransfer extends PrimitiveVectorTransfer<FloatVector, FloatBuffer> {
    FloatVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Float.BYTES, targetPageSize,
                FloatBuffer.allocate(targetPageSize / Float.BYTES), Float.BYTES);
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