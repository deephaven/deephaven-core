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

import java.nio.LongBuffer;

final class LongArrayTransfer extends PrimitiveArrayAndVectorTransfer<long[], long[], LongBuffer> {
    LongArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                     final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Long.BYTES, targetPageSize,
                LongBuffer.allocate(targetPageSize / Long.BYTES), Long.BYTES);
    }

    @Override
    int getSize(final long @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<long[]> data) {
        buffer.put(data.encodedValues);
    }
}
