/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class IntChunkChunk<ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings("unchecked")
    private static final IntChunkChunk EMPTY = new IntChunkChunk<>(new IntChunk[0], 0, 0);

    public static <ATTR extends Any> IntChunkChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <ATTR extends Any> IntChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new IntChunk[capacity];
    }

    public static <ATTR extends Any> IntChunkChunk<ATTR> chunkWrap(IntChunk<ATTR>[] data) {
        return new IntChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> IntChunkChunk<ATTR> chunkWrap(IntChunk<ATTR>[] data, int offset, int capacity) {
        return new IntChunkChunk<>(data, offset, capacity);
    }

    IntChunk<ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    int[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    IntChunkChunk(IntChunk<ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(IntChunk<ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            innerData = new int[newCapacity][];
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
    final void resetInnerCacheItem(int index, IntChunk<ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final IntChunk<ATTR> get(int index) {
        return data[offset + index];
    }

    public final IntChunk<ATTR> getChunk(int index) {
        return get(index);
    }

    public final int get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public IntChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new IntChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
