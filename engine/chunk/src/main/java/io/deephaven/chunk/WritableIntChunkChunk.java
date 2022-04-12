/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableIntChunkChunk<ATTR extends Any> extends IntChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableIntChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableIntChunk[capacity];
    }

    public static <ATTR extends Any> WritableIntChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableIntChunkChunk<ATTR> writableChunkWrap(WritableIntChunk<ATTR>[] data) {
        return new WritableIntChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableIntChunkChunk<ATTR> writableChunkWrap(WritableIntChunk<ATTR>[] data, int offset, int size) {
        return new WritableIntChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableIntChunk<ATTR>[] writableData;

    WritableIntChunkChunk(WritableIntChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableIntChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableIntChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableIntChunk());
    }

    public final void set(int j, int i, int value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableIntChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableIntChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
