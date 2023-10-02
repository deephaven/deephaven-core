/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.DoubleBuffer;

final class DoubleArrayTransfer extends PrimitiveArrayAndVectorTransfer<double[], double[], DoubleBuffer> {
    DoubleArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Double.BYTES, targetPageSize,
                DoubleBuffer.allocate(targetPageSize / Double.BYTES));
    }

    @Override
    void encodeDataForBuffering(final double @NotNull [] data, @NotNull final EncodedData<double[]> encodedData) {
        encodedData.fillRepeated(data, data.length * Double.BYTES, data.length);
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Double.BYTES + getRepeatCount().position() * Integer.BYTES;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = DoubleBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final double @NotNull [] data) {
        buffer.put(data);
    }
}
