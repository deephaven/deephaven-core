/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

final class CodecTransfer<COLUMN_TYPE> extends ObjectTransfer<COLUMN_TYPE> {
    private final ObjectCodec<? super COLUMN_TYPE> codec;

    CodecTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final ObjectCodec<? super COLUMN_TYPE> codec,
            @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        this.codec = codec;
    }

    @Override
    void encodeDataForBuffering(@NotNull COLUMN_TYPE data, @NotNull final EncodedData<Binary> encodedData) {
        Binary encodedValue = Binary.fromConstantByteArray(codec.encode(data));
        encodedData.fillSingle(encodedValue, encodedValue.length());
    }
}
