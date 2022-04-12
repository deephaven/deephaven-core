/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableDoubleChunkChunk<ATTR extends Any> extends DoubleChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableDoubleChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableDoubleChunk[capacity];
    }

    public static <ATTR extends Any> WritableDoubleChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableDoubleChunkChunk<ATTR> writableChunkWrap(WritableDoubleChunk<ATTR>[] data) {
        return new WritableDoubleChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableDoubleChunkChunk<ATTR> writableChunkWrap(WritableDoubleChunk<ATTR>[] data, int offset, int size) {
        return new WritableDoubleChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableDoubleChunk<ATTR>[] writableData;

    WritableDoubleChunkChunk(WritableDoubleChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableDoubleChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableDoubleChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableDoubleChunk());
    }

    public final void set(int j, int i, double value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableDoubleChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableDoubleChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
