/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

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
class PrimitiveTransfer<C extends WritableChunk<Values>, B extends Buffer>
        implements TransferObject<B> {
    private final C chunk;
    private final B buffer;
    private final ColumnSource<?> columnSource;
    private final ChunkSource.FillContext context;
    private boolean hasMoreDataToBuffer;

    <A> PrimitiveTransfer(
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final C chunk,
            @NotNull final B buffer,
            final int targetSize) {
        this.columnSource = columnSource;
        this.chunk = chunk;
        this.buffer = buffer;
        context = columnSource.makeFillContext(targetSize);
    }

    @Override
    public void fetchData(@NotNull final RowSequence rs) {
        columnSource.fillChunk(context, chunk, rs);
        hasMoreDataToBuffer = true;
    }

    @Override
    public int transferAllToBuffer() {
        return transferOnePageToBuffer();
    }

    @Override
    public int transferOnePageToBuffer() {
        if (!hasMoreDataToBuffer()) {
            return 0;
        }
        // Assuming that buffer and chunk are backed by the same array.
        buffer.position(0);
        buffer.limit(chunk.size());
        hasMoreDataToBuffer = false;
        return chunk.size();
    }

    @Override
    public boolean hasMoreDataToBuffer() {
        return hasMoreDataToBuffer;
    }

    @Override
    public B getBuffer() {
        return buffer;
    }

    @Override
    public void close() {
        context.close();
    }
}
