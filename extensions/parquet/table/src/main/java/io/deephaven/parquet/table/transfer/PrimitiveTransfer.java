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
 * PrimitiveTransfer is a generic class that can be used to transfer primitive data types directly from a ColumnSource
 * to a Buffer using {@link ColumnSource#fillChunk(ChunkSource.FillContext, WritableChunk, RowSequence)}.
 */
abstract class PrimitiveTransfer<C extends WritableChunk<Values>, B extends Buffer> implements TransferObject<B> {
    private final C chunk;
    private final B buffer;
    private final ColumnSource<?> columnSource;
    private final RowSequence.Iterator tableRowSetIt;
    private final ChunkSource.FillContext context;
    private final int maxValuesPerPage;

    <A> PrimitiveTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSequence tableRowSet,
            @NotNull final C chunk,
            @NotNull final B buffer,
            final int maxValuesPerPage) {
        this.columnSource = columnSource;
        this.tableRowSetIt = tableRowSet.getRowSequenceIterator();
        this.chunk = chunk;
        this.buffer = buffer;
        Assert.gtZero(maxValuesPerPage, "maxValuesPerPage");
        this.maxValuesPerPage = maxValuesPerPage;
        this.context = columnSource.makeFillContext(maxValuesPerPage);
    }

    @Override
    public final int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        // Fetch one page worth of data from the column source
        final RowSequence rs = tableRowSetIt.getNextRowSequenceWithLength(maxValuesPerPage);
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
    public final B getBuffer() {
        return buffer;
    }

    @Override
    public final void close() {
        context.close();
        tableRowSetIt.close();
    }
}
