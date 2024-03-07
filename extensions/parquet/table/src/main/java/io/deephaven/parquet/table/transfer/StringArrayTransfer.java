//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

final class StringArrayTransfer extends ObjectArrayTransfer<String> {
    StringArrayTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
            final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes);
    }

    @Override
    Binary encodeToBinary(String value) {
        return Binary.fromString(value);
    }
}
