/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class ByteChunkChunk<ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings("unchecked")
    private static final ByteChunkChunk EMPTY = new ByteChunkChunk<>(new ByteChunk[0], 0, 0);

    public static <ATTR extends Any> ByteChunkChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <ATTR extends Any> ByteChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new ByteChunk[capacity];
    }

    public static <ATTR extends Any> ByteChunkChunk<ATTR> chunkWrap(ByteChunk<ATTR>[] data) {
        return new ByteChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> ByteChunkChunk<ATTR> chunkWrap(ByteChunk<ATTR>[] data, int offset, int capacity) {
        return new ByteChunkChunk<>(data, offset, capacity);
    }

    ByteChunk<ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    byte[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    ByteChunkChunk(ByteChunk<ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(ByteChunk<ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            innerData = new byte[newCapacity][];
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
    final void resetInnerCacheItem(int index, ByteChunk<ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final ByteChunk<ATTR> get(int index) {
        return data[offset + index];
    }

    public final ByteChunk<ATTR> getChunk(int index) {
        return get(index);
    }

    public final byte get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public ByteChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ByteChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
