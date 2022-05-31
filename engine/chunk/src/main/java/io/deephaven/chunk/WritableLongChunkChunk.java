/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit WritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;


public class WritableLongChunkChunk<ATTR extends Any> extends LongChunkChunk<ATTR> implements WritableChunkChunk<ATTR> {

    public static <ATTR extends Any> WritableLongChunk<ATTR>[] makeArray(int capacity) {
        //noinspection unchecked
        return new WritableLongChunk[capacity];
    }

    public static <ATTR extends Any> WritableLongChunkChunk<ATTR> makeWritableChunk(int size) {
        return writableChunkWrap(makeArray(size), 0, size);
    }

    public static <ATTR extends Any> WritableLongChunkChunk<ATTR> writableChunkWrap(WritableLongChunk<ATTR>[] data) {
        return new WritableLongChunkChunk<>(data, 0, data.length);
    }

    public static <ATTR extends Any> WritableLongChunkChunk<ATTR> writableChunkWrap(WritableLongChunk<ATTR>[] data, int offset, int size) {
        return new WritableLongChunkChunk<>(data, offset, size);
    }

    /**
     * alias of super.data, but of child type
     */
    WritableLongChunk<ATTR>[] writableData;

    WritableLongChunkChunk(WritableLongChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
        this.writableData = data;
    }

    public final void set(int index, WritableLongChunk<ATTR> value) {
        data[offset + index] = value;
        resetInnerCacheItem(index, value);
    }

    @Override
    public final WritableLongChunk<ATTR> getWritableChunk(int pos) {
        return writableData[offset + pos];
    }

    @Override
    public final void setWritableChunk(int pos, WritableChunk<ATTR> value) {
        set(pos, value.asWritableLongChunk());
    }

    public final void set(int j, int i, long value) {
        innerData[j][innerOffsets[j] + i] = value;
    }

    @Override
    public WritableLongChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new WritableLongChunkChunk<>(writableData, this.offset + offset, capacity);
    }
}
