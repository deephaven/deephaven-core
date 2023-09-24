/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;
import java.nio.IntBuffer;

/**
 * TODO Add comments
 */
abstract class ArrayAndVectorTransfer<T, B extends Buffer> extends VariableWidthTransfer<T, T, B> {
    protected final IntBuffer arrayLengths; // TODO Use better name

    ArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize, buffer);
        this.arrayLengths = IntBuffer.allocate(maxValuesPerPage);
    }

    @Override
    public final IntBuffer getRepeatCount() {
        return arrayLengths;
    }

    @Override
    public int transferOnePageToBuffer() {
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

    @Override
    final boolean addNullToBuffer() {
        // TODO Do we need to add anything to buffer?
        if (!arrayLengths.hasRemaining()) {
            return false;
        }
        arrayLengths.put(QueryConstants.NULL_INT);
        return true;
    }

    // TODO Use better names
    final boolean addEncodedDataToBuffer(@NotNull final EncodedData encodedData) {
        if (!arrayLengths.hasRemaining()) {
            return false;
        }
        if (!copyToBuffer(encodedData.data)) {
            return false;
        }
        arrayLengths.put(encodedData.numBytes);
        return true;
    }

    abstract boolean copyToBuffer(@NotNull final T data);
}

