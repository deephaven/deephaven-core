//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

final class DictEncodedStringVectorTransfer extends DictEncodedStringArrayAndVectorTransfer<ObjectVector<String>> {
    DictEncodedStringVectorTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary) {
        super(columnSource, tableRowSet, targetPageSize, dictionary);
    }

    @Override
    void encodeDataForBuffering(@NotNull ObjectVector<String> data, @NotNull final EncodedData<int[]> encodedData) {
        try (CloseableIterator<String> iter = data.iterator()) {
            Supplier<String> supplier = iter::next;
            encodeDataForBufferingHelper(supplier, data.intSize(), encodedData);
        }
    }
}
