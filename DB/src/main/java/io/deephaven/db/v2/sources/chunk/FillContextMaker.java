package io.deephaven.db.v2.sources.chunk;

import io.deephaven.util.annotations.FinalDefault;

public interface FillContextMaker {
    /**
     * Allocate a new {@link ChunkSource.FillContext} for filling chunks from this
     * {@code FillContextMaker}, typically a {@code ChunkSource}.
     *
     * @param chunkCapacity The maximum size of any {@link WritableChunk} that will be filled with
     *        this context
     * @param sharedContext Shared store of intermediate results.
     * @return A context for use with fill operations
     */
    ChunkSource.FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext);

    /**
     * Allocate a new {@link ChunkSource.FillContext} for filling chunks from this
     * {@code FillContextMaker}, typically a {@link ChunkSource}, without a {@link SharedContext}.
     *
     * @param chunkCapacity The maximum size of any {@link WritableChunk} that will be filled with
     *        this context
     * @return A context for use with fill operations
     */
    @FinalDefault
    default ChunkSource.FillContext makeFillContext(int chunkCapacity) {
        return makeFillContext(chunkCapacity, null);
    }
}
