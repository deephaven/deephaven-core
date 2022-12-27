/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

public interface FillUnordered<ATTR extends Any> {
    /**
     * Populates a contiguous portion of the given destination chunk with data corresponding to the keys from the given
     * {@link LongChunk}.
     * 
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param dest The chunk to be populated according to {@code keys}
     * @param keys A chunk of individual, not assumed to be ordered keys to be fetched
     */
    void fillChunkUnordered(
            @NotNull ChunkSource.FillContext context,
            @NotNull WritableChunk<? super ATTR> dest,
            @NotNull LongChunk<? extends RowKeys> keys);

    /**
     * Populates a contiguous portion of the given destination chunk with prev data corresponding to the keys from the
     * given {@link LongChunk}.
     * 
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param dest The chunk to be populated according to {@code keys}
     * @param keys A chunk of individual, not assumed to be ordered keys to be fetched
     */
    void fillPrevChunkUnordered(
            @NotNull ChunkSource.FillContext context,
            @NotNull WritableChunk<? super ATTR> dest,
            @NotNull LongChunk<? extends RowKeys> keys);

    /**
     * Returns true if this column source can efficiently provide an unordered fill.
     *
     * If this method returns false, then fillChunkUnordered and fillPrevChunkUnordered may throw an
     * UnsupportedOperationException.
     *
     * @return if this column source can provide an unordered fill
     */
    boolean providesFillUnordered();

    static boolean providesFillUnordered(ChunkSource<?> chunkSource) {
        return (chunkSource instanceof FillUnordered) && ((FillUnordered<?>) chunkSource).providesFillUnordered();
    }
}
