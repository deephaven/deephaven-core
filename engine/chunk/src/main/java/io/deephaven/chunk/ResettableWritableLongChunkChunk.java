/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;


public class ResettableWritableLongChunkChunk<ATTR extends Any> extends WritableLongChunkChunk<ATTR> implements ResettableWritableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableWritableLongChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableWritableLongChunkChunk<>();
    }

    private ResettableWritableLongChunkChunk(WritableLongChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableLongChunkChunk() {
        this(WritableLongChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableWritableLongChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableLongChunkChunk<>(writableData, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(WritableChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asWritableLongChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final WritableLongChunk<ATTR>[] typedArray = (WritableLongChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(WritableLongChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.writableData, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(WritableLongChunk<ATTR>[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        final int oldCapacity = this.capacity;
        this.data = data;
        this.writableData = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        resetInnerCache(data, offset, oldCapacity, capacity);
    }
}
