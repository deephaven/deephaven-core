/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.vector.IntVectorColumnWrapper;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

// TODO Add comments
final class IntVectorTransfer extends ArrayAndVectorTransfer<IntVectorColumnWrapper, IntBuffer> {
    IntVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize, IntBuffer.allocate(targetPageSize / Integer.BYTES));
    }

    @Override
    EncodedData encodeDataForBuffering(@NotNull final IntVectorColumnWrapper data) {
        // TODO Add comment
        return new EncodedData(data, data.intSize());
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Integer.BYTES;
    }

    @Override
    boolean copyToBuffer(@NotNull final IntVectorColumnWrapper data) {
        if (data.intSize() > buffer.remaining()) {
            return false;
        }
        data.iterator().forEachRemaining((int value) -> buffer.put(value));
        return true;
    }
}