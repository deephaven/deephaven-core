/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.LongBuffer;
import java.time.Instant;


// TODO Ask Ryan if we need to do write something like ReinterpretUtils.instantToLongSource since that's what we do
// for non array/vector instant transfers.
final class InstantArrayTransfer extends PrimitiveArrayAndVectorTransfer<Instant[], Instant[], LongBuffer> {
    InstantArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
            final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Long.BYTES, targetPageSize,
                LongBuffer.allocate(targetPageSize / Long.BYTES));
    }

    @Override
    void encodeDataForBuffering(final Instant @NotNull [] data, @NotNull final EncodedData<Instant[]> encodedData) {
        // We lie here about encoding the data and instead send back an Instant array with number of bytes equal to
        // the final length after we convert them to Longs. We later do the conversion from Instant -> Long
        // inside {@link #copyToBuffer} below. This is done to avoid creating a temporary array of Longs.
        encodedData.fillRepeated(data, data.length * Long.BYTES, data.length);
    }

    @Override
    int getNumBytesBuffered() {
        return buffer.position() * Long.BYTES + getRepeatCount().position() * Integer.BYTES;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = LongBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final Instant @NotNull [] data) {
        for (Instant t : data) {
            buffer.put(DateTimeUtils.epochNanos(t));
        }
    }
}
