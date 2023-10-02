/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class IntArrayTransfer extends PrimitiveArrayAndVectorTransfer<int[], int[], IntBuffer> {
    IntArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                     final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES));
    }

    @Override
    void encodeDataForBuffering(final int @NotNull [] data, @NotNull final EncodedData<int[]> encodedData) {
        encodedData.fillRepeated(data, data.length * Integer.BYTES, data.length);
    }

    @Override
    int getNumBytesBuffered() {
        return (buffer.position() + repeatCounts.position()) * Integer.BYTES;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final int @NotNull [] data) {
        buffer.put(data);
    }
}
