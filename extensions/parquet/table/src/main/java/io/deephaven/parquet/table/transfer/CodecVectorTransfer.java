//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used to encode vectors of objects using a codec provided on construction. The difference between using this class and
 * {@link CodecTransfer} is that this class encodes each element of the vector individually whereas
 * {@link CodecTransfer} will encode the entire vector as a single value.
 */
final class CodecVectorTransfer<VALUE_TYPE> extends ObjectVectorTransfer<VALUE_TYPE> {
    private final ObjectCodec<? super VALUE_TYPE> codec;

    CodecVectorTransfer(final @NotNull ColumnSource<?> columnSource,
            @NotNull final ObjectCodec<? super VALUE_TYPE> codec,
            final @NotNull RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        this.codec = codec;
    }

    @Override
    Binary encodeToBinary(VALUE_TYPE value) {
        return Binary.fromConstantByteArray(codec.encode(value));
    }
}
