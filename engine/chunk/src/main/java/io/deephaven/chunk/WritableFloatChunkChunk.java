/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableFloatChunkChunk<ATTR extends Any> extends FloatChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableFloatChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableFloatChunk[capacity];
    }

    public static <ATTR extends Any> WritableFloatChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableFloatChunkChunk<ATTR> writableChunkWrap(WritableFloatChunk<ATTR>[] data) {
        return new WritableFloatChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableFloatChunkChunk<ATTR> writableChunkWrap(WritableFloatChunk<ATTR>[] data, int offset, int size) {
        return new WritableFloatChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableFloatChunk<ATTR>[] writableData;

    WritableFloatChunkChunk(WritableFloatChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableFloatChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableFloatChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableFloatChunk());
    }

    public final void set(int j, int i, float value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableFloatChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableFloatChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
