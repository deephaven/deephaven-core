/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

final class DictEncodedStringArrayTransfer extends DictEncodedStringTransferBase<String[]> {
    DictEncodedStringArrayTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary, final int nullPos) {
        super(columnSource, tableRowSet, targetPageSize, dictionary, nullPos);
    }

    @Override
    void encodeDataForBuffering(@NotNull String @NotNull [] data) {
        final class ArrayDataSupplier implements Supplier<String> {
            private final String[] data;
            private int pos = 0;

            private ArrayDataSupplier(String[] data) {
                this.data = data;
            }

            @Override
            public String get() {
                return data[pos++];
            }
        }
        dictEncodingHelper(new ArrayDataSupplier(data), data.length);
    }
}
