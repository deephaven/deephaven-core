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
 * Used as a base class of transfer objects for types like strings or big integers that need specialized encoding.
 */
abstract class ObjectTransfer<COLUMN_TYPE> extends VariableWidthTransfer<COLUMN_TYPE, Binary, Binary[]> {
    /**
     * Number of values (null or non-null) added to the buffer
     */
    private int bufferedDataCount;
    /**
     * Total number of bytes buffered
     */
    private int numBytesBuffered;

    ObjectTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                   final int targetPageSize) {
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
    final boolean isBufferEmpty() {
        return bufferedDataCount == 0;
    }

    @Override
    final boolean addNullToBuffer() {
        if (bufferedDataCount == maxValuesPerPage) {
            return false;
        }
        buffer[bufferedDataCount++] = null;
        return true;
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData<Binary> data, final boolean force) {
        if (force && bufferedDataCount != 0) {
            // This should never happen, because "force" is only set by the caller when adding the very first object
            //noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
            return false;
        }
        if (bufferedDataCount == maxValuesPerPage) {
            return false;
        }
        buffer[bufferedDataCount++] = data.encodedValues;
        numBytesBuffered += data.numBytes;
        return true;
    }
}
