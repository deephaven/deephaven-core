/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class ShortChunkChunk<ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static final ShortChunkChunk EMPTY = new ShortChunkChunk<>(new ShortChunk[0], 0, 0);

    public static <ATTR extends Any> ShortChunkChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <ATTR extends Any> ShortChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new ShortChunk[capacity];
    }

    public static <ATTR extends Any> ShortChunkChunk<ATTR> chunkWrap(ShortChunk<ATTR>[] data) {
        return new ShortChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> ShortChunkChunk<ATTR> chunkWrap(ShortChunk<ATTR>[] data, int offset, int capacity) {
        return new ShortChunkChunk<>(data, offset, capacity);
    }

    ShortChunk<ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    short[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    ShortChunkChunk(ShortChunk<ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(ShortChunk<ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            innerData = new short[newCapacity][];
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
    final void resetInnerCacheItem(int index, ShortChunk<ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final ShortChunk<ATTR> get(int index) {
        return data[offset + index];
    }

    public final ShortChunk<ATTR> getChunk(int index) {
        return get(index);
    }

    public final short get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public ShortChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ShortChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
