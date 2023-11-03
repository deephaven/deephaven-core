package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ChunkBase;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

/**
 * A transfer object base class for primitive types that can be cast to {@code int} without loss of precision.
 * Uses {@link IntBuffer} as the buffer type.
 */
// TODO Need a better name
abstract class Int32CastablePrimitiveTransfer<CHUNK_TYPE extends ChunkBase<Values>> extends PrimitiveTransferNonArrayBacked<CHUNK_TYPE, IntBuffer> {
    Int32CastablePrimitiveTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet, final int targetPageSizeInBytes) {
        super(columnSource, tableRowSet,
                IntBuffer.allocate(Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Integer.BYTES))),
                Math.toIntExact(Math.min(tableRowSet.size(), targetPageSizeInBytes / Integer.BYTES)));
    }
}
