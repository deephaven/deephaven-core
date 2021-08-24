package io.deephaven.db.v2.sources.chunk;

import io.deephaven.util.annotations.FinalDefault;

public interface GetContextMaker {
    /**
     * Allocate a new {@link ChunkSource.GetContext} for retrieving chunks from this
     * {@code GetContextMaker}, typically a {@code ChunkSource}.
     *
     * @param chunkCapacity The maximum size required for any {@link WritableChunk} allocated as
     *        part of the result.
     * @param sharedContext Shared store of intermediate results.
     * @return A context for use with get operations
     */
    ChunkSource.GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext);

    /**
     * Allocate a new {@link ChunkSource.GetContext} for retrieving chunks from this
     * {@code FillContextMaker}, typically a {@code ChunkSource} without a {@link SharedContext}.
     *
     * @param chunkCapacity The maximum size required for any {@link WritableChunk} allocated as
     *        part of the result.
     * @return A context for use with get operations
     */
    @FinalDefault
    default ChunkSource.GetContext makeGetContext(int chunkCapacity) {
        return makeGetContext(chunkCapacity, null);
    }
}
