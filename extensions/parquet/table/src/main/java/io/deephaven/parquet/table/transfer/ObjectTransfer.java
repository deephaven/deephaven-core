/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Used as a base class of transfer objects for types like strings or big integers that need specialized encoding, and
 * thus we need to enforce page size limits while writing.
 */
abstract class ObjectTransfer<T> extends VariableWidthTransfer<T, Binary, Binary[]> {
    final private int bufferSize;
    private int bufferedDataCount;
    private int numBytesBuffered;

    ObjectTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                   final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize, targetPageSize, new Binary[targetPageSize]);
        bufferSize = targetPageSize;
        bufferedDataCount = 0;
        numBytesBuffered = 0;
        // TODO Add comment about arguments
    }

    @Override
    public int transferOnePageToBuffer() {
        // Clear any old buffered data
        if (bufferedDataCount != 0) {
            Arrays.fill(buffer, 0, bufferedDataCount, null);
            bufferedDataCount = 0;
            numBytesBuffered = 0;
        }
        // Fill the buffer with data from the table
        transferOnePageToBufferHelper();
        return bufferedDataCount;
    }

    @Override
    final int getNumBytesBuffered() {
        return numBytesBuffered;
    }

    @Override
    final boolean addNullToBuffer() {
        if (bufferedDataCount == bufferSize) {
            return false;
        }
        buffer[bufferedDataCount++] = null;
        return true;
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData encodedData) {
        if (bufferedDataCount == bufferSize) {
            return false;
        }
        buffer[bufferedDataCount++] = encodedData.data;
        numBytesBuffered += encodedData.numBytes;
        return true;
    }
}
