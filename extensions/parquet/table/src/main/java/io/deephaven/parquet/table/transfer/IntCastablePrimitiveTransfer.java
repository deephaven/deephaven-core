/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.base.verify.Assert;
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
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.GetContext context;
    private final int maxValuesPerPage;

    IntCastablePrimitiveTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                                 final int targetPageSize) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.maxValuesPerPage = Math.toIntExact(Math.min(tableRowSet.size(), targetPageSize / Integer.BYTES));
        Assert.gtZero(maxValuesPerPage, "maxValuesPerPage");
        this.buffer = IntBuffer.allocate(maxValuesPerPage);
        context = columnSource.makeGetContext(maxValuesPerPage);
    }

    @Override
    public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        buffer.clear();
        // Fetch one page worth of data from the column source
        final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength((long) maxValuesPerPage);
        // noinspection unchecked
        chunk = (T) columnSource.getChunk(context, rs);
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
    public final boolean hasMoreDataToBuffer() {
        return tableRowSetIt.hasMore();
    }

    @Override
    public final IntBuffer getBuffer() {
        return buffer;
    }

    @Override
    public final void close() {
        context.close();
        tableRowSetIt.close();
    }
}
