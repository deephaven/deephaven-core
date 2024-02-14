/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntArrayTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.DoubleBuffer;

final class DoubleArrayTransfer extends PrimitiveArrayAndVectorTransfer<double[], double[], DoubleBuffer> {
    DoubleArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                     final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes / Double.BYTES, targetPageSizeInBytes,
                DoubleBuffer.allocate(targetPageSizeInBytes / Double.BYTES), Double.BYTES);
    }

    @Override
    int getSize(final double @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = DoubleBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<double[]> data) {
        buffer.put(data.encodedValues);
    }
}
