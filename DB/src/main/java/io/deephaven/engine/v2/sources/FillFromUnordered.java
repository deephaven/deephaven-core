package io.deephaven.engine.v2.sources;

import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
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
            @NotNull WritableChunkSink.FillFromContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends Attributes.RowKeys> keys);
}
