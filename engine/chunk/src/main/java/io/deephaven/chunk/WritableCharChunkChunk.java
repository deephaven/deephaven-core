package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableCharChunkChunk<ATTR extends Any> extends CharChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableCharChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableCharChunk[capacity];
    }

    public static <ATTR extends Any> WritableCharChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableCharChunkChunk<ATTR> writableChunkWrap(WritableCharChunk<ATTR>[] data) {
        return new WritableCharChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableCharChunkChunk<ATTR> writableChunkWrap(WritableCharChunk<ATTR>[] data, int offset, int size) {
        return new WritableCharChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableCharChunk<ATTR>[] writableData;

    WritableCharChunkChunk(WritableCharChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableCharChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableCharChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableCharChunk());
    }

    public final void set(int j, int i, char value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableCharChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableCharChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
