/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

 import io.deephaven.base.verify.Assert;
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
    /**
     * Number of values added to the buffer
     */
    private int bufferedDataCount;
    /**
     * Total number of bytes buffered
     */
    private int numBytesBuffered;

    ObjectArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize, targetPageSize, new Binary[targetPageSize]);
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

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData<Binary[]> data, final boolean force) {
        if (force && (!repeatCounts.hasRemaining() || bufferedDataCount != 0)) {
            // This should never happen, because "force" set by caller when adding the very first array/vector
            //noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
            return false;
        }
        if (!repeatCounts.hasRemaining()) {
            return false;
        }
        final int numEncodedValues = data.numValues;
        if (bufferedDataCount + numEncodedValues > maxValuesPerPage) {
            if (force) {
                // Resize the buffer, if needed. Assuming the buffer is empty, verified earlier
                if (buffer.length < numEncodedValues) {
                    buffer = new Binary[numEncodedValues];
                }
            } else {
                return false;
            }
        }
        for (final Binary val : data.encodedValues) {
            buffer[bufferedDataCount++] = val;
        }
        numBytesBuffered += data.numBytes;
        repeatCounts.put(numEncodedValues);
        return true;
    }
}
