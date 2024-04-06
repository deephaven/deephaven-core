//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkBase;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;

/**
 * This is a generic class used to transfer primitive data types from a {@link ColumnSource} to a {@link Buffer} using
 * {@link ColumnSource#getChunk(ChunkSource.GetContext, RowSequence)} and then copying values into the buffer.
 */
abstract class GettingPrimitiveTransfer<CHUNK_TYPE extends ChunkBase<Values>, BUFFER_TYPE extends Buffer>
        implements TransferObject<BUFFER_TYPE> {
    protected CHUNK_TYPE chunk;
    protected final BUFFER_TYPE buffer;
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.GetContext context;
    private final int targetElementsPerPage;

    GettingPrimitiveTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            final BUFFER_TYPE buffer,
            final int targetElementsPerPage) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.buffer = buffer;
        Assert.gtZero(targetElementsPerPage, "targetElementsPerPage");
        this.targetElementsPerPage = targetElementsPerPage;
        context = columnSource.makeGetContext(targetElementsPerPage);
    }

    @Override
    public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        buffer.clear();
        // Fetch one page worth of data from the column source
        final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength((long) targetElementsPerPage);
        // noinspection unchecked
        chunk = (CHUNK_TYPE) columnSource.getChunk(context, rs);
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
    public final BUFFER_TYPE getBuffer() {
        return buffer;
    }

    @Override
    public final void close() {
        context.close();
        tableRowSetIt.close();
    }
}
