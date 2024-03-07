//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

final class DictEncodedStringArrayTransfer extends DictEncodedStringArrayAndVectorTransfer<String[]> {
    private final ArrayDataSupplier<String> supplier;

    DictEncodedStringArrayTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary) {
        super(columnSource, tableRowSet, targetPageSize, dictionary);
        supplier = new ArrayDataSupplier<>();
    }

    @Override
    void encodeDataForBuffering(@NotNull final String @NotNull [] data, @NotNull final EncodedData<int[]> encodedData) {
        supplier.fill(data);
        encodeDataForBufferingHelper(supplier, data.length, encodedData);
    }
}
