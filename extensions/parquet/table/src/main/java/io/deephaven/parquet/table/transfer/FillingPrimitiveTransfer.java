/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.Buffer;

/**
 * This is a generic class that can be used to transfer primitive data types directly from a {@link ColumnSource} to a
 * {@link Buffer} using {@link ColumnSource#fillChunk(ChunkSource.FillContext, WritableChunk, RowSequence)}. This class
 * can only be used if the {@link WritableChunk} and {@link Buffer} are backed by the same array.
 */
abstract class FillingPrimitiveTransfer<CHUNK_TYPE extends WritableChunk<Values>, BUFFER_TYPE extends Buffer>
        implements TransferObject<BUFFER_TYPE> {
    private final CHUNK_TYPE chunk;
    private final BUFFER_TYPE buffer;
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.FillContext context;
    private final int targetElementsPerPage;

    <A> FillingPrimitiveTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final CHUNK_TYPE chunk,
            @NotNull final BUFFER_TYPE buffer,
            final int targetElementsPerPage) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.chunk = chunk;
        this.buffer = buffer;
        Assert.gtZero(targetElementsPerPage, "targetElementsPerPage");
        this.targetElementsPerPage = targetElementsPerPage;
        this.context = columnSource.makeFillContext(targetElementsPerPage);
    }

    @Override
    public final int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        // Fetch one page worth of data from the column source
        final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(targetElementsPerPage);
        columnSource.fillChunk(context, chunk, rs);
        // Assuming that buffer and chunk are backed by the same array.
        buffer.position(0);
        buffer.limit(chunk.size());
        return chunk.size();
    }

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
