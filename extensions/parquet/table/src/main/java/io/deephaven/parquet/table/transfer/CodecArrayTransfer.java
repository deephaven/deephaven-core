/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used to encode arrays of objects using a codec provided on construction. The difference between using this class and
 * {@link CodecTransfer} is that this class encodes each element of the array individually whereas {@link CodecTransfer}
 * will encode the entire array as a single value.
 */
final class CodecArrayTransfer<VALUE_TYPE> extends ObjectArrayTransfer<VALUE_TYPE> {
    private final ObjectCodec<? super VALUE_TYPE> codec;

    CodecArrayTransfer(final @NotNull ColumnSource<?> columnSource,
            @NotNull final ObjectCodec<? super VALUE_TYPE> codec,
            final @NotNull RowSequence tableRowSet, final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet, targetPageSizeInBytes);
        this.codec = codec;
    }

    @Override
    Binary encodeToBinary(VALUE_TYPE value) {
        return Binary.fromConstantByteArray(codec.encode(value));
    }
}
