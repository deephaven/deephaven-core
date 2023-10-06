/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;
import java.util.function.Supplier;

final class DictEncodedStringArrayTransfer extends DictEncodedStringArrayAndVectorTransfer<String[]> {
    private final StringArrayDataSupplier supplier;

    DictEncodedStringArrayTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary, final int nullPos) {
        super(columnSource, tableRowSet, targetPageSize, dictionary, nullPos);
        supplier = new StringArrayDataSupplier();
    }

    static final class StringArrayDataSupplier implements Supplier<String> {
        private String[] data;
        private int pos = 0;

        void fill(final @NotNull String[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public String get() {
            return data[pos++];
        }
    }

    @Override
    void encodeDataForBuffering(@NotNull String @NotNull [] data, @NotNull final EncodedData<IntBuffer> encodedData) {
        supplier.fill(data);
        dictEncodingHelper(supplier, data.length, encodedData);
    }
}
