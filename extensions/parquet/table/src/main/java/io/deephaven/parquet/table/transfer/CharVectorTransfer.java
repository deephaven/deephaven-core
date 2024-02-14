/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfChar;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.CharVector;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class CharVectorTransfer extends PrimitiveVectorTransfer<CharVector, IntBuffer> {
    CharVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                       final int targetPageSizeInBytes) {
        // We encode primitive chars as primitive ints
        super(columnSource, tableRowSet, targetPageSizeInBytes / Integer.BYTES, targetPageSizeInBytes,
                IntBuffer.allocate(targetPageSizeInBytes / Integer.BYTES), Integer.BYTES);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<CharVector> data) {
        try (final CloseablePrimitiveIteratorOfChar dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((char value) -> buffer.put(value));
        }
    }
}