/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

final class CodecTransfer<T> extends EncodedTransfer<T> {
    private final ObjectCodec<? super T> codec;

    CodecTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final ObjectCodec<? super T> codec,
            @NotNull final RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        this.codec = codec;
    }

    @Override
    EncodedData encodeDataForBuffering(@NotNull T data) {
        Binary encodedValue = Binary.fromConstantByteArray(codec.encode(data));
        return new EncodedData(encodedValue, encodedValue.length());
    }
}
