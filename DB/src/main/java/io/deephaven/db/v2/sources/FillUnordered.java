package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

public interface FillUnordered {
    /**
     * Populates a contiguous portion of the given destination chunk with data corresponding to the keys from the given
     * {@link LongChunk}.
     * 
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param dest The chunk to be populated according to {@code keys}
     * @param keys A chunk of individual, not assumed to be ordered keys to be fetched
     */
    void fillChunkUnordered(
            @NotNull ColumnSource.FillContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends KeyIndices> keys);

    /**
     * Populates a contiguous portion of the given destination chunk with prev data corresponding to the keys from the
     * given {@link LongChunk}.
     * 
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param dest The chunk to be populated according to {@code keys}
     * @param keys A chunk of individual, not assumed to be ordered keys to be fetched
     */
    void fillPrevChunkUnordered(
            @NotNull ColumnSource.FillContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends KeyIndices> keys);
}
