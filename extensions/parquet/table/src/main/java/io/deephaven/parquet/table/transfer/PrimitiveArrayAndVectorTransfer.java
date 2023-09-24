/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;

/**
 * TODO Add comments
 */
abstract class PrimitiveArrayAndVectorTransfer<T, B extends Buffer> extends ArrayAndVectorTransfer<T, T, B> {
    PrimitiveArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer);
    }

    @Override
    final public int transferOnePageToBuffer() {
        // Clear any old buffered data
        buffer.clear();
        arrayLengths.clear();
        // Fill the buffer with data from the table
        transferOnePageToBufferHelper();
        // Prepare buffer for reading
        buffer.flip();
        arrayLengths.flip();
        return buffer.limit();
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData encodedData) {
        if (!arrayLengths.hasRemaining()) {
            return false;
        }
        if (buffer.position() == 0 && encodedData.numValues > buffer.remaining()) {
            // Resize the buffer if the first array/vector doesn't fit
            resizeBuffer(encodedData.numValues);
        } else if (encodedData.numValues > buffer.remaining()) {
            return false;
        }
        copyToBuffer(encodedData.data);
        arrayLengths.put(encodedData.numValues);
        return true;
    }

    // TODO Add comments why not boolean
    abstract void copyToBuffer(@NotNull final T data);

    abstract void resizeBuffer(@NotNull final int length);
}

