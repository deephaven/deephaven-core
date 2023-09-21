/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.chunk.ChunkBase;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;
import java.nio.IntBuffer;

/**
 * A transfer object base class for primitive types that can be cast to int without loss of precision.
 */
abstract class IntCastablePrimitiveTransfer<T extends ChunkBase<Values>> implements TransferObject<IntBuffer> {
    protected T chunk;
    protected final IntBuffer buffer;
    private final ColumnSource<?> columnSource;
    private final ChunkSource.GetContext context;

    IntCastablePrimitiveTransfer(ColumnSource<?> columnSource, int targetSize) {
        this.columnSource = columnSource;
        this.buffer = IntBuffer.allocate(targetSize);
        context = columnSource.makeGetContext(targetSize);
    }

    @Override
    final public void fetchData(@NotNull final RowSequence rs) {
        // noinspection unchecked
        chunk = (T) columnSource.getChunk(context, rs);
    }

    @Override
    final public int transferAllToBuffer() {
        return transferOnePageToBuffer();
    }

    @Override
    final public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        buffer.clear();
        // Assuming that all the fetched data will fit in one page. This is because page count is accurately
        // calculated for non variable-width types. Check ParquetTableWriter.getTargetRowsPerPage for more details.
        copyAllFromChunkToBuffer();
        buffer.flip();
        int ret = chunk.size();
        chunk = null;
        return ret;
    }

    /**
     * Helper method to copy all data from {@code this.chunk} to {@code this.buffer}. The buffer should be cleared
     * before calling this method and is positioned for a {@link Buffer#flip()} after the call.
     */
    abstract void copyAllFromChunkToBuffer();

    @Override
    final public boolean hasMoreDataToBuffer() {
        return (chunk != null);
    }

    @Override
    final public IntBuffer getBuffer() {
        return buffer;
    }

    @Override
    final public void close() {
        context.close();
    }
}
