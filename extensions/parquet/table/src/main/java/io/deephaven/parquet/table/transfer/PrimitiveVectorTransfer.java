/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;
import java.nio.IntBuffer;

// TODO Add comments
abstract class PrimitiveVectorTransfer<T extends Vector<?>, B extends Buffer>
        extends PrimitiveArrayAndVectorTransfer<T, T, B> {
    private final int numBytesPerValue;

    PrimitiveVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer, final int numBytesPerValue) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer);
        this.numBytesPerValue = numBytesPerValue;
    }

    @Override
    final EncodedData encodeDataForBuffering(@NotNull final T data) {
        // TODO Add comment
        return new EncodedData(data, data.intSize(), data.intSize() * numBytesPerValue);
    }

    @Override
    final int getNumBytesBuffered() {
        return buffer.position() * numBytesPerValue;
    }
}
