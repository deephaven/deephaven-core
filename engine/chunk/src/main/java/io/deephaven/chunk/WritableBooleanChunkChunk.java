/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableBooleanChunkChunk<ATTR extends Any> extends BooleanChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableBooleanChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableBooleanChunk[capacity];
    }

    public static <ATTR extends Any> WritableBooleanChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableBooleanChunkChunk<ATTR> writableChunkWrap(WritableBooleanChunk<ATTR>[] data) {
        return new WritableBooleanChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableBooleanChunkChunk<ATTR> writableChunkWrap(WritableBooleanChunk<ATTR>[] data, int offset, int size) {
        return new WritableBooleanChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableBooleanChunk<ATTR>[] writableData;

    WritableBooleanChunkChunk(WritableBooleanChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableBooleanChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableBooleanChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableBooleanChunk());
    }

    public final void set(int j, int i, boolean value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableBooleanChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableBooleanChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
