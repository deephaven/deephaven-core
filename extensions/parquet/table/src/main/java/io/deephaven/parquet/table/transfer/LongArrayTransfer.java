/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;

final class LongArrayTransfer extends PrimitiveArrayAndVectorTransfer<long[], long[], LongBuffer> {
    LongArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                     final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Long.BYTES, targetPageSize,
                LongBuffer.allocate(targetPageSize / Long.BYTES));
    }

    @Override
    void encodeDataForBuffering(final long @NotNull [] data) {
        encodedData.fill(data, data.length, data.length * Long.BYTES);
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Integer.BYTES;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final long @NotNull [] data) {
        buffer.put(data);
    }
}
