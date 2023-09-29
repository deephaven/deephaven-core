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

    ObjectVectorTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                         final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    final void encodeDataForBuffering(final @NotNull ObjectVector<T> data) {
        int numObjects = data.intSize();
        if (binaryEncodedValues == null || numObjects > binaryEncodedValues.length) {
            binaryEncodedValues = new Binary[numObjects];
        }
        int numBytesEncoded = 0;
        try (CloseableIterator<T> iter = data.iterator()) {
            for (int i = 0; i < numObjects; i++) {
                T value = iter.next();
                if (value == null) {
                    binaryEncodedValues[i] = null;
                } else {
                    binaryEncodedValues[i] = encodeToBinary(value);
                    numBytesEncoded += binaryEncodedValues[i].length();
                }
            }
        }
        encodedData.fill(binaryEncodedValues, numObjects, numBytesEncoded);
    }

    abstract Binary encodeToBinary(T value);
}
