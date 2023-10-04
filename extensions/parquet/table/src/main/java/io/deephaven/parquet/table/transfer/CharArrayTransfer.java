/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class CharArrayTransfer extends PrimitiveArrayAndVectorTransfer<char[], char[], IntBuffer> {
    CharArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        // We encode characters as integers
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES));
    }

    @Override
    void encodeDataForBuffering(final char @NotNull [] data, @NotNull final EncodedData<char[]> encodedData) {
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
    void copyToBuffer(final char @NotNull [] data) {
        for (char c : data) {
            buffer.put((int) c);
        }
    }
}
