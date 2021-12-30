/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class BooleanChunkChunk<ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings("unchecked")
    private static final BooleanChunkChunk EMPTY = new BooleanChunkChunk<>(new BooleanChunk[0], 0, 0);

    public static <ATTR extends Any> BooleanChunkChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <ATTR extends Any> BooleanChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new BooleanChunk[capacity];
    }

    public static <ATTR extends Any> BooleanChunkChunk<ATTR> chunkWrap(BooleanChunk<ATTR>[] data) {
        return new BooleanChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> BooleanChunkChunk<ATTR> chunkWrap(BooleanChunk<ATTR>[] data, int offset, int capacity) {
        return new BooleanChunkChunk<>(data, offset, capacity);
    }

    BooleanChunk<ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    boolean[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    BooleanChunkChunk(BooleanChunk<ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(BooleanChunk<ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            innerData = new boolean[newCapacity][];
            innerOffsets = new int[newCapacity];
        }
        for (int ii = 0; ii < newCapacity; ++ii) {
            resetInnerCacheItem(ii, data[ii + offset]);
        }
        for (int ii = newCapacity; ii < oldCapacity; ++ii) {
            // Be friendly to the garbage collector
            innerData[ii] = null;
            innerOffsets[ii] = 0;  // to be nice
        }
    }

    /**
     * Update a specific cached "inner" data structures.
     */
    final void resetInnerCacheItem(int index, BooleanChunk<ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final BooleanChunk<ATTR> get(int index) {
        return data[offset + index];
    }

    public final BooleanChunk<ATTR> getChunk(int index) {
        return get(index);
    }

    public final boolean get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public BooleanChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new BooleanChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
