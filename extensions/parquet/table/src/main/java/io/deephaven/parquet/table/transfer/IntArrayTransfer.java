/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

// TODO Add comments
final class IntArrayTransfer extends PrimitiveArrayAndVectorTransfer<int[], int[], IntBuffer> {
    IntArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize, IntBuffer.allocate(targetPageSize / Integer.BYTES));
    }

    @Override
    EncodedData encodeDataForBuffering(@NotNull final int[] data) {
        // TODO Add comment
        return new EncodedData(data, data.length, data.length * Integer.BYTES);
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Integer.BYTES;
    }

    @Override
    void resizeBuffer(@NotNull final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
   void copyToBuffer(@NotNull final int[] data) {
        buffer.put(data);
    }
}
