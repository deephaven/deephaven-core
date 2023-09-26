/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.IntVectorDirect;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

// TODO Add comments
final class IntVectorTransfer extends PrimitiveVectorTransfer<IntVectorDirect, IntBuffer> {
    IntVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(targetPageSize / Integer.BYTES), Integer.BYTES);
    }
    @Override
    void resizeBuffer(@NotNull final int length) {
        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final IntVectorDirect data) {
        data.iterator().forEachRemaining((int value) -> buffer.put(value));
    }
}