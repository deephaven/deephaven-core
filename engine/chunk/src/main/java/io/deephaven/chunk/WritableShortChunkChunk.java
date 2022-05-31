/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableShortChunkChunk<ATTR extends Any> extends ShortChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableShortChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableShortChunk[capacity];
    }

    public static <ATTR extends Any> WritableShortChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableShortChunkChunk<ATTR> writableChunkWrap(WritableShortChunk<ATTR>[] data) {
        return new WritableShortChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableShortChunkChunk<ATTR> writableChunkWrap(WritableShortChunk<ATTR>[] data, int offset, int size) {
        return new WritableShortChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableShortChunk<ATTR>[] writableData;

    WritableShortChunkChunk(WritableShortChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableShortChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableShortChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableShortChunk());
    }

    public final void set(int j, int i, short value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableShortChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableShortChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
