package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;


public class ResettableWritableCharChunkChunk<ATTR extends Any> extends WritableCharChunkChunk<ATTR> implements ResettableWritableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableWritableCharChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableWritableCharChunkChunk<>();
    }

    private ResettableWritableCharChunkChunk(WritableCharChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableCharChunkChunk() {
        this(WritableCharChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableWritableCharChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableCharChunkChunk<>(writableData, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(WritableChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asWritableCharChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final WritableCharChunk<ATTR>[] typedArray = (WritableCharChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(WritableCharChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.writableData, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(WritableCharChunk<ATTR>[] data, int offset, int capacity) {
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
