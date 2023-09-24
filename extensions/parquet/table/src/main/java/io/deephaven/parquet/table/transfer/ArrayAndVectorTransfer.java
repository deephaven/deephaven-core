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
abstract class ArrayAndVectorTransfer<T, B extends Buffer> extends VariableWidthTransfer<T, B> {
    B buffer;
    IntBuffer arrayLengths; // TODO Use better name

    ArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int maxValuesPerPage, final int targetPageSize, @NotNull final B buffer) {
        super(columnSource, tableRowSet, maxValuesPerPage, targetPageSize);
        this.buffer = buffer;
        this.arrayLengths = IntBuffer.allocate(maxValuesPerPage);
    }

    @Override
    public final B getBuffer() {
        return buffer;
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
    void addNullToBuffer() {
        // TODO Do we need to add anything to buffer?
        arrayLengths.put(QueryConstants.NULL_INT);
    }

    // TODO Use better names
    void addEncodedDataToBuffer(@NotNull final VariableWidthTransfer<T, B>.EncodedData encodedData) {
        copyToBuffer(encodedData.data);
        arrayLengths.put(encodedData.numBytes);
    }

    abstract void copyToBuffer(@NotNull final T data);
}

