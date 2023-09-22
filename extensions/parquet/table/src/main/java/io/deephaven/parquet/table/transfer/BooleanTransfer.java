/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class BooleanTransfer extends PrimitiveTransfer<WritableByteChunk<Values>, ByteBuffer> {

    static BooleanTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet, int targetPageSize) {
        final int maxValuesPerPage = targetPageSize << 3;            // = targetPageSize * 8 because 8 booleans per byte
        final byte[] backingArray = new byte[maxValuesPerPage];
        return new BooleanTransfer(
                columnSource,
                tableRowSet,
                WritableByteChunk.writableChunkWrap(backingArray),
                ByteBuffer.wrap(backingArray),
                maxValuesPerPage);
    }

    private BooleanTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableByteChunk<Values> chunk,
            @NotNull final ByteBuffer buffer,
            int maxValuesPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, maxValuesPerPage);
    }
}
