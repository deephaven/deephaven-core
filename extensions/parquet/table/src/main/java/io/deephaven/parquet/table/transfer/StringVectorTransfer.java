package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.vector.ObjectVectorColumnWrapper;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

final class StringVectorTransfer extends ObjectArrayAndVectorTransfer<ObjectVectorColumnWrapper<String>> {

    StringVectorTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    EncodedData encodeDataForBuffering(final @NotNull ObjectVectorColumnWrapper<String> data) {
        int numStrings = data.intSize();
        Binary[] binaryEncodedValues = new Binary[numStrings];
        int numBytesEncoded = 0;
        try (CloseableIterator<String> iter = data.iterator()) {
            for (int i = 0; i < numStrings; i++) {
                String value = iter.next();
                if (value == null) {
                    binaryEncodedValues[i] = null;
                } else {
                    binaryEncodedValues[i] = Binary.fromString(value);
                    numBytesEncoded += binaryEncodedValues[i].length();
                }
            }
        }
        return new EncodedData(binaryEncodedValues, numStrings, numBytesEncoded);
    }
}
