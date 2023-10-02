/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used as a base class of transfer objects for vectors of types like strings or big integers that need specialized
 * encoding.
 */
abstract class ObjectVectorTransfer<T> extends ObjectArrayAndVectorTransfer<ObjectVector<T>> {
    /**
     * Used as a temporary buffer for storing encoded data for a single row before it is copied to the main buffer.
     * Allocated lazily because of the high cost of construction of Binary objects.
     */
    private Binary[] encodedDataBuf;

    ObjectVectorTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                         final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        encodedDataBuf = null;
    }

    @Override
    final void encodeDataForBuffering(final @NotNull ObjectVector<T> data, @NotNull final EncodedData<Binary[]> encodedData) {
        int numObjects = data.intSize();
        if (encodedDataBuf == null || numObjects > encodedDataBuf.length) {
            encodedDataBuf = new Binary[numObjects];
        }
        int numBytesEncoded = 0;
        try (CloseableIterator<T> iter = data.iterator()) {
            for (int i = 0; i < numObjects; i++) {
                T value = iter.next();
                if (value == null) {
                    encodedDataBuf[i] = null;
                } else {
                    encodedDataBuf[i] = encodeToBinary(value);
                    numBytesEncoded += encodedDataBuf[i].length();
                }
            }
        }
        encodedData.fillRepeated(encodedDataBuf, numBytesEncoded, numObjects);
    }

    abstract Binary encodeToBinary(T value);
}
