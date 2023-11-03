/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;

/**
 * Used as a base class of transfer objects for arrays/vectors of primitive types.
 */
abstract class PrimitiveArrayAndVectorTransfer<COLUMN_TYPE, ENCODED_COLUMN_TYPE, BUFFER_TYPE extends Buffer>
        extends ArrayAndVectorTransfer<COLUMN_TYPE, ENCODED_COLUMN_TYPE, BUFFER_TYPE> {

    private final int numBytesPerValue;

    PrimitiveArrayAndVectorTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetElementsPerPage, final int targetPageSizeInBytes,
            @NotNull final BUFFER_TYPE buffer, final int numBytesPerValue) {
        super(columnSource, tableRowSet, targetElementsPerPage, targetPageSizeInBytes, buffer);
        this.numBytesPerValue = numBytesPerValue;
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

    @Override
    void encodeDataForBuffering(@NotNull final COLUMN_TYPE data,
            @NotNull final EncodedData<ENCODED_COLUMN_TYPE> encodedData) {
        // No encoding needed here because we can calculate how many bytes will be needed per encoded value.
        // So we store the reference to data as is and do any required encoding later while copying to buffer.
        // This is done to avoid creating a temporary copy of encoded values here.
        int numValues = getSize(data);
        // noinspection unchecked
        encodedData.fillRepeated((ENCODED_COLUMN_TYPE) data, numValues * numBytesPerValue, numValues);
    }

    /**
     * Get the size of primitive array/vector, called from inside {@link #encodeDataForBuffering}. Not needed for
     * classes which override the method and do their own encoding, like dictionary encoded strings.
     *
     * @param data the array/vector
     */
    int getSize(@NotNull final COLUMN_TYPE data) {
        throw new UnsupportedOperationException("getSize() not implemented for " + getClass().getSimpleName());
    }

    @Override
    final int getNumBytesBuffered() {
        return buffer.position() * numBytesPerValue;
    }

    final boolean addEncodedDataToBuffer(@NotNull final EncodedData<ENCODED_COLUMN_TYPE> data, boolean force) {
        if (force && (repeatCounts.position() != 0 || buffer.position() != 0)) {
            // This should never happen, because "force" is only set by the caller when adding the very first
            // array/vector
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted();
            return false;
        }
        if (!repeatCounts.hasRemaining()) {
            return false;
        }
        if (buffer.position() + data.numValues > targetElementsPerPage) {
            if (force) {
                // Assuming buffer is empty here, verified earlier
                if (buffer.limit() < data.numValues) {
                    resizeBuffer(data.numValues);
                }
            } else {
                return false;
            }
        }
        copyToBuffer(data);
        repeatCounts.put(data.numValues);
        return true;
    }

    /**
     * Copy the encoded values to the buffer. This function should be called after checking that there is enough space
     * in the buffer.
     */
    abstract void copyToBuffer(@NotNull final EncodedData<ENCODED_COLUMN_TYPE> data);

    /**
     * Resize the underlying page buffer, needed in case of overflow when transferring the first array/vector.
     */
    abstract void resizeBuffer(final int length);
}

