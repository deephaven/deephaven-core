/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class ByteArrayTransfer extends PrimitiveArrayAndVectorTransfer<byte[], byte[], IntBuffer> {
    ByteArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        // We encode characters as integers
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES));
    }

    @Override
    void encodeDataForBuffering(final byte @NotNull [] data, @NotNull final EncodedData<byte[]> encodedData) {
        encodedData.fillRepeated(data, data.length * Integer.BYTES, data.length);
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Integer.BYTES;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final byte @NotNull [] data) {
        for (byte b : data) {
            buffer.put((int) b);
        }
    }
}
