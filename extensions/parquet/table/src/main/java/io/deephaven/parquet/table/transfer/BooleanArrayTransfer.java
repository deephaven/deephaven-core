/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class BooleanArrayTransfer extends PrimitiveArrayAndVectorTransfer<Boolean[], Boolean[], ByteBuffer> {
    // We encode booleans as bytes here and bit pack them with 8 booleans per byte at the time of writing.
    // Therefore, we need to allocate (targetPageSize * 8) bytes for the buffer.
    private static final int BYTES_NEEDED_PER_ENCODED_BOOLEAN_VALUE = 1;
    private static final int NUM_BIT_PACKED_BOOLEANS_PER_BYTE = 8;
    BooleanArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize * NUM_BIT_PACKED_BOOLEANS_PER_BYTE,
                targetPageSize * NUM_BIT_PACKED_BOOLEANS_PER_BYTE,
                ByteBuffer.allocate(targetPageSize * NUM_BIT_PACKED_BOOLEANS_PER_BYTE),
                BYTES_NEEDED_PER_ENCODED_BOOLEAN_VALUE);
    }

    @Override
    int getSize(final Boolean @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = ByteBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final @NotNull EncodedData<Boolean[]> data) {
        for (Boolean b : data.encodedValues) {
            buffer.put(BooleanUtils.booleanAsByte(b));
        }
    }
}
