/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

final class StringVectorTransfer extends ObjectVectorTransfer<String> {
    StringVectorTransfer(final @NotNull ColumnSource<?> columnSource, final @NotNull RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    Binary encodeToBinary(String value) {
        return Binary.fromString(value);
    }
}
