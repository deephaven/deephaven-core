package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public interface FillFromUnordered {
    /**
     * Populates a contiguous portion of the given destination chunk with data corresponding to the keys from the given
     * {@link LongChunk}.
     * 
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param dest The chunk to be populated according to {@code keys}
     * @param keys A chunk of individual, not assumed to be ordered keys to be fetched
     */
    void fillFromChunkUnordered(
            @NotNull ChunkSink.FillFromContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys);
}
