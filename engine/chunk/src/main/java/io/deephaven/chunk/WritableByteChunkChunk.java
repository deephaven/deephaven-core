/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableByteChunkChunk<ATTR extends Any> extends ByteChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableByteChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableByteChunk[capacity];
    }

    public static <ATTR extends Any> WritableByteChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableByteChunkChunk<ATTR> writableChunkWrap(WritableByteChunk<ATTR>[] data) {
        return new WritableByteChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableByteChunkChunk<ATTR> writableChunkWrap(WritableByteChunk<ATTR>[] data, int offset, int size) {
        return new WritableByteChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableByteChunk<ATTR>[] writableData;

    WritableByteChunkChunk(WritableByteChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableByteChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableByteChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableByteChunk());
    }

    public final void set(int j, int i, byte value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableByteChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableByteChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
