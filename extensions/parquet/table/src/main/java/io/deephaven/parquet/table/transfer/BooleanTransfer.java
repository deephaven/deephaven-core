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

final class BooleanTransfer extends FillingPrimitiveTransfer<WritableByteChunk<Values>, ByteBuffer> {
    // We encode booleans as bytes here and bit pack them with 8 booleans per byte at the time of writing.
    // Therefore, max values per page are (targetPageSizeInBytes * 8).
    static BooleanTransfer create(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSet tableRowSet,
                                  int targetPageSizeInBytes) {
        final int NUM_BIT_PACKED_BOOLEANS_PER_BYTE = 8;
        final int targetElementsPerPage = Math.toIntExact(Math.min(tableRowSet.size(),
                (long) targetPageSizeInBytes * NUM_BIT_PACKED_BOOLEANS_PER_BYTE));
        final byte[] backingArray = new byte[targetElementsPerPage];
        return new BooleanTransfer(
                columnSource,
                tableRowSet,
                WritableByteChunk.writableChunkWrap(backingArray),
                ByteBuffer.wrap(backingArray),
                targetElementsPerPage);
    }

    private BooleanTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final WritableByteChunk<Values> chunk,
            @NotNull final ByteBuffer buffer,
            int targetElementsPerPage) {
        super(columnSource, tableRowSet, chunk, buffer, targetElementsPerPage);
    }
}
