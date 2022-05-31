/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableObjectChunkChunk<T, ATTR extends Any> extends ObjectChunkChunk<T, ATTR> implements WritableChunkChunk<ATTR> {

    public static <T, ATTR extends Any> WritableObjectChunk<T, ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableObjectChunk[capacity];
    }

    public static <T, ATTR extends Any> WritableObjectChunkChunk<T, ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <T, ATTR extends Any> WritableObjectChunkChunk<T, ATTR> writableChunkWrap(WritableObjectChunk<T, ATTR>[] data) {
        return new WritableObjectChunkChunk<>(data, 0, data.length);
    }

    public static <T, ATTR extends Any> WritableObjectChunkChunk<T, ATTR> writableChunkWrap(WritableObjectChunk<T, ATTR>[] data, int offset, int size) {
        return new WritableObjectChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableObjectChunk<T, ATTR>[] writableData;

    WritableObjectChunkChunk(WritableObjectChunk<T, ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableObjectChunk<T, ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableObjectChunk<T, ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableObjectChunk());
    }

    public final void set(int j, int i, T value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableObjectChunkChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableObjectChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
