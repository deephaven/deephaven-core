/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used as a base class of transfer objects for arrays of types like strings or big integers that need specialized
 * encoding.
 */
abstract class ObjectArrayTransfer<T> extends ObjectArrayAndVectorTransfer<T[]> {
    /**
     * Used as a temporary buffer for storing encoded data for a single row before it is copied to the main buffer.
     * Allocated lazily because of the high cost of construction of Binary objects.
     */
    private Binary[] encodedDataBuf;

    ObjectArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                        final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        encodedDataBuf = null;
    }

    @Override
    final void encodeDataForBuffering(final T @NotNull [] data, @NotNull final EncodedData<Binary[]> encodedData) {
        int numObjects = data.length;
        if (encodedDataBuf == null || numObjects > encodedDataBuf.length) {
            encodedDataBuf = new Binary[numObjects];
        }
        int numBytesEncoded = 0;
        for (int i = 0; i < numObjects; i++) {
            T value = data[i];
            if (value == null) {
                encodedDataBuf[i] = null;
            } else {
                encodedDataBuf[i] = encodeToBinary(value);
                numBytesEncoded += encodedDataBuf[i].length();
            }
        }
        encodedData.fillRepeated(encodedDataBuf, numBytesEncoded, numObjects);
    }

    /**
     * Encode a single value to binary
     */
    abstract Binary encodeToBinary(T value);
}
