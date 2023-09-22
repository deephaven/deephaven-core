/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

final class CodecTransfer<T> extends EncodedTransfer<T> {
    private final ObjectCodec<? super T> codec;

    CodecTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final ObjectCodec<? super T> codec,
            final int targetPageSize) {
        super(columnSource, targetPageSize);
        this.codec = codec;
    }

    @Override
    Binary encodeToBinary(T value) {
        return Binary.fromConstantByteArray(codec.encode(value));
    }
}
