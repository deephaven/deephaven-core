package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * A generic object intended to serve as a thin wrapper around a multidimensional array region.
 */
public abstract class ChunkChunkBase<ATTR extends Any> implements ChunkChunk<ATTR> {
    /**
     * The Chunk-of-Chunk's storage is the sub-range of the underlying array defined by [offset,
     * offset + capacity). It is illegal to access the underlying array outside of this range.
     */
    int offset;
    int capacity;
    /**
     * Useful data in the chunk-of-chunks is in the sub-range of the underlying array defined by
     * [offset, offset + size). It is illegal to set size < 0 or size > capacity.
     */
    int size;

    ChunkChunkBase(int arrayLength, int offset, int capacity) {
        ChunkUtils.checkArrayArgs(arrayLength, offset, capacity);
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }

    @Override
    public final int size() {
        return size;
    }
}
