package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;


final class StringArrayTransfer extends ObjectArrayAndVectorTransfer<String[]> {

    StringArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    EncodedData encodeDataForBuffering(final @NotNull String[] data) {
        int numStrings = data.length;
        Binary[] binaryEncodedValues = new Binary[numStrings];
        int numBytesEncoded = 0;
        for (int i = 0; i < numStrings; i++) {
            String value = data[i];
            if (value == null) {
                binaryEncodedValues[i] = null;
            } else {
                binaryEncodedValues[i] = Binary.fromString(value);
                numBytesEncoded += binaryEncodedValues[i].length();
            }
        }
        return new EncodedData(binaryEncodedValues, numStrings, numBytesEncoded);
    }
}
