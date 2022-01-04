/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class ObjectChunkChunk<T, ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings("unchecked")
    private static final ObjectChunkChunk EMPTY = new ObjectChunkChunk<>(new ObjectChunk[0], 0, 0);

    public static <T, ATTR extends Any> ObjectChunkChunk<T, ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <T, ATTR extends Any> ObjectChunk<T, ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new ObjectChunk[capacity];
    }

    public static <T, ATTR extends Any> ObjectChunkChunk<T, ATTR> chunkWrap(ObjectChunk<T, ATTR>[] data) {
        return new ObjectChunkChunk<>(data, 0, data.length);
    }

    public static <T, ATTR extends Any> ObjectChunkChunk<T, ATTR> chunkWrap(ObjectChunk<T, ATTR>[] data, int offset, int capacity) {
        return new ObjectChunkChunk<>(data, offset, capacity);
    }

    ObjectChunk<T, ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    T[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    ObjectChunkChunk(ObjectChunk<T, ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(ObjectChunk<T, ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            //noinspection unchecked
            innerData = (T[][])new Object[newCapacity][];
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
    final void resetInnerCacheItem(int index, ObjectChunk<T, ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final ObjectChunk<T, ATTR> get(int index) {
        return data[offset + index];
    }

    public final ObjectChunk<T, ATTR> getChunk(int index) {
        return get(index);
    }

    public final T get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public ObjectChunkChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ObjectChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
