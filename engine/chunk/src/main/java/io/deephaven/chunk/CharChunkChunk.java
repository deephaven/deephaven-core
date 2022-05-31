package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class CharChunkChunk<ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings("unchecked")
    private static final CharChunkChunk EMPTY = new CharChunkChunk<>(new CharChunk[0], 0, 0);

    public static <ATTR extends Any> CharChunkChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <ATTR extends Any> CharChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new CharChunk[capacity];
    }

    public static <ATTR extends Any> CharChunkChunk<ATTR> chunkWrap(CharChunk<ATTR>[] data) {
        return new CharChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> CharChunkChunk<ATTR> chunkWrap(CharChunk<ATTR>[] data, int offset, int capacity) {
        return new CharChunkChunk<>(data, offset, capacity);
    }

    CharChunk<ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    char[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    CharChunkChunk(CharChunk<ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(CharChunk<ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            innerData = new char[newCapacity][];
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
    final void resetInnerCacheItem(int index, CharChunk<ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final CharChunk<ATTR> get(int index) {
        return data[offset + index];
    }

    public final CharChunk<ATTR> getChunk(int index) {
        return get(index);
    }

    public final char get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public CharChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new CharChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
