//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class IntArrayTransfer extends PrimitiveArrayAndVectorTransfer<int[], int[], IntBuffer> {
    IntArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Integer.BYTES, targetPageSizeInBytes,
                IntBuffer.allocate(targetPageSizeInBytes / Integer.BYTES), Integer.BYTES);
    }

    @Override
    int getSize(final int @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<int[]> data) {
        buffer.put(data.encodedValues);
    }
}
