/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;


abstract class ObjectArrayTransfer<T> extends ObjectArrayAndVectorTransfer<T[]> {
        ObjectArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
                        final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    final EncodedData encodeDataForBuffering(final @NotNull T[] data) {
        int numStrings = data.length;
        Binary[] binaryEncodedValues = new Binary[numStrings];
        int numBytesEncoded = 0;
        for (int i = 0; i < numStrings; i++) {
            T value = data[i];
            if (value == null) {
                binaryEncodedValues[i] = null;
            } else {
                binaryEncodedValues[i] = encodeToBinary(value);
                numBytesEncoded += binaryEncodedValues[i].length();
            }
        }
        return new EncodedData(binaryEncodedValues, numStrings, numBytesEncoded);
    }

    abstract Binary encodeToBinary(T value);
}
