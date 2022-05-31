/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

public class FloatChunkChunk<ATTR extends Any> extends ChunkChunkBase<ATTR> implements ChunkChunk<ATTR> {
    @SuppressWarnings("unchecked")
    private static final FloatChunkChunk EMPTY = new FloatChunkChunk<>(new FloatChunk[0], 0, 0);

    public static <ATTR extends Any> FloatChunkChunk<ATTR> getEmptyChunk() {
        //noinspection unchecked
        return EMPTY;
    }

    public static <ATTR extends Any> FloatChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new FloatChunk[capacity];
    }

    public static <ATTR extends Any> FloatChunkChunk<ATTR> chunkWrap(FloatChunk<ATTR>[] data) {
        return new FloatChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> FloatChunkChunk<ATTR> chunkWrap(FloatChunk<ATTR>[] data, int offset, int capacity) {
        return new FloatChunkChunk<>(data, offset, capacity);
    }

    FloatChunk<ATTR>[] data;
    /**
     * innerData[i] is a cached copy of data[i].data used for faster two-dimensional access.
     */
    float[][] innerData;
    /**
     * innerOffsets[i] is a cached copy of data[i].offset used for faster two-dimensional access.
     */
    int[] innerOffsets;

    FloatChunkChunk(FloatChunk<ATTR>[] data, int offset, int capacity) {
        super(data.length, offset, capacity);
        this.data = data;
        resetInnerCache(data, offset, 0, capacity);
    }

    /**
     * Update cached "inner" data structures.
     */
    final void resetInnerCache(FloatChunk<ATTR>[] data, int offset, int oldCapacity, int newCapacity) {
        if (innerData == null || innerData.length < newCapacity) {
            innerData = new float[newCapacity][];
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
    final void resetInnerCacheItem(int index, FloatChunk<ATTR> chunk) {
        if (chunk == null) {
            innerData[index] = null;
            innerOffsets[index] = 0;
        } else {
            innerData[index] = chunk.data;
            innerOffsets[index] = chunk.offset;
        }
    }

    public final FloatChunk<ATTR> get(int index) {
        return data[offset + index];
    }

    public final FloatChunk<ATTR> getChunk(int index) {
        return get(index);
    }

    public final float get(int j, int i) {
        return innerData[j][innerOffsets[j] + i];
    }

    @Override
    public FloatChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new FloatChunkChunk<>(data, this.offset + offset, capacity);
    }

    // region AsType
    // endregion AsType
}
