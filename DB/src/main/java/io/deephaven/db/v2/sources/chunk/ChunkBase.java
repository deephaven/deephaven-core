package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * A generic object intended to serve as a thin wrapper around an array region.
 */
public abstract class ChunkBase<ATTR extends Any> implements Chunk<ATTR> {
    /**
     * The Chunk's storage is the sub-range of the underlying array defined by [offset, offset +
     * capacity). It is illegal to access the underlying array outside of this range.
     */
    int offset;
    int capacity;
    /**
     * Useful data data in the chunk is in the sub-range of the underlying array defined by [offset,
     * offset + size). It is illegal to set size < 0 or size > capacity.
     */
    int size;

    ChunkBase(int arrayLength, int offset, int capacity) {
        ChunkUtils.checkArrayArgs(arrayLength, offset, capacity);
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }

    @Override
    public final int size() {
        return size;
    }

    /**
     * DO NOT CALL THIS INTERNAL METHOD. If you want to set a size, call
     * {@link WritableChunk#setSize}. That method is the only legal caller of this method in the
     * entire system.
     */
    public final void internalSetSize(int newSize, long password) {
        if (password != -7025656774858671822L) {
            throw new UnsupportedOperationException(
                "DO NOT CALL THIS INTERNAL METHOD. Instead call WritableChunk.setSize()");
        }
        if (newSize < 0 || newSize > capacity) {
            throw new IllegalArgumentException(
                String.format("size %d is incompatible with capacity %d", newSize, capacity));
        }

        this.size = newSize;
    }

    /**
     * DO NOT CALL THIS INTERNAL METHOD. Call {@link WritableChunk#capacity()} That method is the
     * only legal caller of this method in the entire system.
     */
    public final int internalCapacity(long password) {
        if (password != 1837055652467547514L) {
            throw new UnsupportedOperationException(
                "DO NOT CALL THIS INTERNAL METHOD. Instead call WritableChunk.capacity()");
        }
        return capacity;
    }
}
