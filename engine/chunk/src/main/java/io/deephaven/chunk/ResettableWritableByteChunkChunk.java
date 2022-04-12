/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;


public class ResettableWritableByteChunkChunk<ATTR extends Any> extends WritableByteChunkChunk<ATTR> implements ResettableWritableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableWritableByteChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableWritableByteChunkChunk<>();
    }

    private ResettableWritableByteChunkChunk(WritableByteChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableByteChunkChunk() {
        this(WritableByteChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableWritableByteChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableByteChunkChunk<>(writableData, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(WritableChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asWritableByteChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final WritableByteChunk<ATTR>[] typedArray = (WritableByteChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(WritableByteChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.writableData, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(WritableByteChunk<ATTR>[] data, int offset, int capacity) {
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
