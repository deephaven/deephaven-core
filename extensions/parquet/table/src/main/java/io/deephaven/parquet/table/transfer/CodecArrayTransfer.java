/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

// TODO Add comment explaining the difference between this class and CodecTransfer
final class CodecArrayTransfer<T> extends ObjectArrayTransfer<T> {
    private final ObjectCodec<? super T> codec;

    CodecArrayTransfer(final @NotNull ColumnSource<?> columnSource, @NotNull final ObjectCodec<? super T> codec,
            final @NotNull RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        this.codec = codec;
    }

    @Override
    Binary encodeToBinary(T value) {
        return Binary.fromConstantByteArray(codec.encode(value));
    }
}
