/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;

/**
 * Used as a base class of transfer objects for arrays/vectors of primitive types.
 */
abstract class PrimitiveArrayAndVectorTransfer<T, E, B extends Buffer> extends ArrayAndVectorTransfer<T, E, B> {
    PrimitiveArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer);
    }

    @Override
    public int transferOnePageToBuffer() {
        // Clear any old buffered data
        buffer.clear();
        repeatCounts.clear();
        // Fill the buffer with data from the table
        transferOnePageToBufferHelper();
        // Prepare buffer for reading
        buffer.flip();
        repeatCounts.flip();
        return buffer.limit();
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData data) {
        if (!repeatCounts.hasRemaining()) {
            return false;
        }
        if (buffer.position() == 0 && data.numValues > buffer.remaining()) {
            // Resize the buffer if the first array/vector doesn't fit
            resizeBuffer(data.numValues);
        } else if (data.numValues > buffer.remaining()) {
            return false;
        }
        copyToBuffer(data.encodedValues);
        repeatCounts.put(data.numValues);
        return true;
    }

    /**
     * Copy the encoded values to the buffer. This function should be called after checking that there is enough space
     * in the buffer.
     */
    abstract void copyToBuffer(@NotNull final E data);

    /**
     * Resize the underlying page buffer, needed in case of overflow when transferring the first array/vector.
     */
    abstract void resizeBuffer(final int length);
}

