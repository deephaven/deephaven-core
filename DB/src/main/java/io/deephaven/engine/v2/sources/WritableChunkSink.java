package io.deephaven.engine.v2.sources;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.structures.RowSequence;
import org.jetbrains.annotations.NotNull;

public interface WritableChunkSink<ATTR extends Attributes.Any> extends ChunkSource<ATTR> {
    FillFromContext DEFAULT_FILL_FROM_INSTANCE = new FillFromContext() {};

    interface FillFromContext extends Context {
    }

    /**
     * Fills the ChunkSink with data from the source, with data corresponding to the keys from the given
     * {@link RowSequence}.
     * 
     * @param context A context containing all mutable/state related data used in writing the Chunk.
     * @param src The source of the data {@code rowSequence}
     * @param rowSequence An {@link RowSequence} representing the keys to be written
     */
    void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends ATTR> src,
            @NotNull RowSequence rowSequence);

    /**
     * Fills the ChunkSink with data from the source, with data corresponding to the keys from the given key chunk.
     * 
     * @param context A context containing all mutable/state related data used in writing the Chunk.
     * @param src The source of the data {@code RowSequence}
     * @param keys A {@link LongChunk} representing the keys to be written
     */
    void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends ATTR> src,
            @NotNull LongChunk<Attributes.RowKeys> keys);

    /**
     * Make a context suitable for the {@link WritableChunkSink#fillFromChunk} method.
     */
    FillFromContext makeFillFromContext(int chunkCapacity);
}
