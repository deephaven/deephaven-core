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
 import java.util.function.Supplier;

/**
 * Used as a base class of arrays/vectors of transfer objects for types like strings or big integers that need
 * specialized encoding.
 * @param <COLUMN_TYPE> The type of the data in the column, could be an array/vector
 * @param <VALUE_TYPE> The type of the values in the array/vector
 */
abstract class ObjectArrayAndVectorTransfer<COLUMN_TYPE, VALUE_TYPE>
        extends ArrayAndVectorTransfer<COLUMN_TYPE, Binary[], Binary[]> {
    /**
     * Number of values added to the buffer
     */
    private int bufferedDataCount;
    /**
     * Total number of bytes buffered
     */
    private int numBytesBuffered;

    /**
     * Used as a temporary buffer for storing references to binary encoded values for a single row before it is copied
     * to the main buffer.
     */
    private Binary[] encodedDataBuf;
    private int encodedDataBufLen;

    ObjectArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                                 final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize, targetPageSize, new Binary[targetPageSize]);
        bufferedDataCount = 0;
        numBytesBuffered = 0;

        encodedDataBuf = new Binary[targetPageSize];
        encodedDataBufLen = 0;
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

    final void encodeDataForBufferingHelper(@NotNull final Supplier<VALUE_TYPE> objectSupplier, final int numObjects,
                                            @NotNull final EncodedData<Binary[]> encodedData) {
        // Allocate a new buffer if needed, or clear the existing one
        if (numObjects > encodedDataBuf.length) {
            encodedDataBuf = new Binary[numObjects];
        } else {
            Arrays.fill(encodedDataBuf, 0, encodedDataBufLen, null);
            encodedDataBufLen = 0;
        }
        int numBytesEncoded = 0;
        for (int i = 0; i < numObjects; i++) {
            VALUE_TYPE value = objectSupplier.get();
            if (value == null) {
                encodedDataBuf[i] = null;
            } else {
                encodedDataBuf[i] = encodeToBinary(value);
                numBytesEncoded += encodedDataBuf[i].length();
            }
        }
        encodedDataBufLen = numObjects;
        encodedData.fillRepeated(encodedDataBuf, numBytesEncoded, numObjects);
    }

    /**
     * Encode a single value to binary
     */
    abstract Binary encodeToBinary(VALUE_TYPE value);

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData<Binary[]> data, final boolean force) {
        if (force && (repeatCounts.position() != 0 || bufferedDataCount != 0)) {
            // This should never happen, because "force" is only set by the caller when adding the very first
            // array/vector
            //noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
            return false;
        }
        if (!repeatCounts.hasRemaining()) {
            return false;
        }
        final int numEncodedValues = data.numValues;
        if (bufferedDataCount + numEncodedValues > targetElementsPerPage) {
            if (force) {
                // Resize the buffer, if needed. Assuming the buffer is empty, verified earlier
                if (buffer.length < numEncodedValues) {
                    buffer = new Binary[numEncodedValues];
                }
            } else {
                return false;
            }
        }
        for (int i = 0; i < numEncodedValues; i++) {
            buffer[bufferedDataCount++] = data.encodedValues[i];
        }
        numBytesBuffered += data.numBytes;
        repeatCounts.put(numEncodedValues);
        return true;
    }
}
