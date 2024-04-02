//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayTransfer and run "./gradlew replicateParquetTransferObjects" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class ByteArrayTransfer extends PrimitiveArrayAndVectorTransfer<byte[], byte[], IntBuffer> {
    ByteArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        // We encode primitive bytes as primitive ints
        super(columnSource, tableRowSet, targetPageSizeInBytes / Integer.BYTES, targetPageSizeInBytes,
                IntBuffer.allocate(targetPageSizeInBytes / Integer.BYTES), Integer.BYTES);
    }

    @Override
    int getSize(final byte @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<byte[]> data) {
        for (byte value : data.encodedValues) {
            buffer.put(value);
        }
    }
}
