/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

final class StringTransfer extends ObjectTransfer<String> {
    StringTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
    }

    @Override
    void encodeDataForBuffering(@NotNull String data, @NotNull final EncodedData<Binary> encodedData) {
        Binary encodedValue = Binary.fromString(data);
        encodedData.fillSingle(encodedValue, encodedValue.length());
    }
}
