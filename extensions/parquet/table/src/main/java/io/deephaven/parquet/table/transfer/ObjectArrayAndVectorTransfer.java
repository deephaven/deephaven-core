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
 * TODO: Add comments
 */
abstract class ObjectArrayAndVectorTransfer<T> extends ArrayAndVectorTransfer<T, Binary[], Binary[]> {
    private int bufferSize;
    private int bufferedDataCount;
    private int numBytesBuffered;

    ObjectArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize, targetPageSize, new Binary[targetPageSize]);
        bufferSize = targetPageSize;
        bufferedDataCount = 0;
        numBytesBuffered = 0;
        // TODO Add comment about arguments
    }

    @Override
    public final int transferOnePageToBuffer() {
        // Clear any old buffered data
        if (bufferedDataCount != 0) {
            Arrays.fill(buffer, 0, bufferedDataCount, null);
            bufferedDataCount = 0;
            numBytesBuffered = 0;
            arrayLengths.clear();
        }
        // Fill the buffer with data from the table
        transferOnePageToBufferHelper();
        arrayLengths.flip();
        return bufferedDataCount;
    }

    @Override
    final int getNumBytesBuffered() {
        return numBytesBuffered;
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData encodedData) {
        if (!arrayLengths.hasRemaining()) {
            return false;
        }
        int numEncodedValues = encodedData.numValues;
        if (bufferedDataCount == 0 && numEncodedValues > bufferSize) {
            // Resize the buffer if the first array/vector doesn't fit
            bufferSize = numEncodedValues;
            buffer = new Binary[bufferSize];
        } else if (numEncodedValues > bufferSize - bufferedDataCount) {
            return false;
        }
        Binary[] binaryEncodedValues = encodedData.data;
        for (final Binary val : binaryEncodedValues) {
            buffer[bufferedDataCount++] = val;
        }
        numBytesBuffered += encodedData.numBytes;
        arrayLengths.put(numEncodedValues);
        return true;
    }
}
