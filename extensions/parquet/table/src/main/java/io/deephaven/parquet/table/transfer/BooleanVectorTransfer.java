/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.BooleanUtils;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class BooleanVectorTransfer extends PrimitiveVectorTransfer<ObjectVector<Boolean>, ByteBuffer> {
    // We encode booleans as bytes here and bit pack them with 8 booleans per byte at the time of writing.
    // Therefore, we need to allocate (targetPageSizeInBytes * 8) bytes for the buffer.
    private static final int BYTES_NEEDED_PER_ENCODED_BOOLEAN_VALUE = 1;
    private static final int NUM_BIT_PACKED_BOOLEANS_PER_BYTE = 8;
    BooleanVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                          final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes * NUM_BIT_PACKED_BOOLEANS_PER_BYTE,
                targetPageSizeInBytes * NUM_BIT_PACKED_BOOLEANS_PER_BYTE,
                ByteBuffer.allocate(targetPageSizeInBytes * NUM_BIT_PACKED_BOOLEANS_PER_BYTE),
                BYTES_NEEDED_PER_ENCODED_BOOLEAN_VALUE);
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = ByteBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<ObjectVector<Boolean>> data) {
        try (final CloseableIterator<Boolean> dataIterator = data.encodedValues.iterator()) {
            dataIterator.forEachRemaining((Boolean b) -> buffer.put(BooleanUtils.booleanAsByte(b)));
        }
    }
}
