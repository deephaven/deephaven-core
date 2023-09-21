/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

class StringTransfer extends EncodedTransfer<String> {
    public StringTransfer(
            @NotNull final ColumnSource<?> columnSource,
            final int maxValuesPerPage,
            final int targetPageSize) {
        super(columnSource, maxValuesPerPage, targetPageSize);
    }

    @Override
    Binary encodeToBinary(String value) {
        return Binary.fromString(value);
    }
}
