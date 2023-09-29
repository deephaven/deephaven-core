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
        ObjectArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                        final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    final void encodeDataForBuffering(final T[] data) {
        int numObjects = data.length;
        if (binaryEncodedValues == null || numObjects > binaryEncodedValues.length) {
            binaryEncodedValues = new Binary[numObjects];
        }
        int numBytesEncoded = 0;
        for (int i = 0; i < numObjects; i++) {
            T value = data[i];
            if (value == null) {
                binaryEncodedValues[i] = null;
            } else {
                binaryEncodedValues[i] = encodeToBinary(value);
                numBytesEncoded += binaryEncodedValues[i].length();
            }
        }
        encodedData.fill(binaryEncodedValues, numObjects, numBytesEncoded);
    }

    /**
     * Encode a single value to binary
     */
    abstract Binary encodeToBinary(T value);
}
