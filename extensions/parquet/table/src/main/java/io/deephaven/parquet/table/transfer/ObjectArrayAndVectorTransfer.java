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
 * Used as a base class of arrays/vectors of transfer objects for types like strings or big integers that need
 * specialized encoding.
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
    }

    @Override
    public final int transferOnePageToBuffer() {
        // Clear any old buffered data
        if (bufferedDataCount != 0) {
            Arrays.fill(buffer, 0, bufferedDataCount, null);
            bufferedDataCount = 0;
            numBytesBuffered = 0;
            repeatCounts.clear();
        }
        // Fill the buffer with data from the table
        transferOnePageToBufferHelper();
        repeatCounts.flip();
        return bufferedDataCount;
    }

    @Override
    final int getNumBytesBuffered() {
        return numBytesBuffered;
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData data) {
        if (!repeatCounts.hasRemaining()) {
            return false;
        }
        int numEncodedValues = data.numValues;
        if (bufferedDataCount == 0 && numEncodedValues > bufferSize) {
            // Resize the buffer if the first array/vector doesn't fit
            bufferSize = numEncodedValues;
            buffer = new Binary[bufferSize];
        } else if (numEncodedValues > bufferSize - bufferedDataCount) {
            return false;
        }
        for (final Binary val : data.encodedValues) {
            buffer[bufferedDataCount++] = val;
        }
        numBytesBuffered += data.numBytes;
        repeatCounts.put(numEncodedValues);
        return true;
    }
}
