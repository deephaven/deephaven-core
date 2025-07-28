//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * An AutoCloseable holder for a shared context and related FillContexts.
 */
public class ContextHolder implements Context {
    private final int chunkSize;
    private final SharedContext sharedContext;
    private final ChunkSource.FillContext[] fillContexts;
    private final ResettableWritableChunk<Values>[] resettableChunks;

    public ContextHolder(final int chunkSize, @NotNull final ColumnSource<?>... columnSources) {
        this.chunkSize = chunkSize;
        sharedContext = SharedContext.makeSharedContext();
        fillContexts = new ChunkSource.FillContext[columnSources.length];
        // noinspection unchecked
        resettableChunks = new ResettableWritableChunk[columnSources.length];
        for (int i = 0; i < columnSources.length; i++) {
            // TODO: random q -sometimes the SharedContext arg below is ignored? e.g.
            // io.deephaven.engine.table.impl.sources.ViewColumnSource.makeFillContext
            fillContexts[i] = columnSources[i].makeFillContext(chunkSize, sharedContext);
            // noinspection resource
            resettableChunks[i] = columnSources[i].getChunkType().makeResettableWritableChunk();
        }
    }

    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Gets the FillContext for the {@code nth} column source.
     *
     * @param n The index of the column source
     * @return The FillContext for the {@code nth} column source
     */
    public ChunkSource.FillContext getFillContext(int n) {
        return fillContexts[n];
    }

    /**
     * Gets the ResettableWritableChunk for the {@code nth} column source.
     *
     * @param n The index of the column source
     * @return The ResettableWritableChunk for the {@code nth} column source
     */
    public ResettableWritableChunk<Values> getResettableChunk(final int n) {
        return resettableChunks[n];
    }

    /**
     * Reset the {@link #sharedContext SharedContext}.
     */
    public void resetSharedContext() {
        sharedContext.reset();
    }

    @Override
    public void close() {
        for (final SafeCloseable closeable : fillContexts) {
            closeable.close();
        }
        for (final SafeCloseable closeable : resettableChunks) {
            closeable.close();
        }
        sharedContext.close();
    }
}
