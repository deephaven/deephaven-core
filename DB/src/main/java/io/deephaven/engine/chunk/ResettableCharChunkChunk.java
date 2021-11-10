package io.deephaven.engine.chunk;
import io.deephaven.engine.chunk.Attributes.Any;
import io.deephaven.engine.v2.utils.ChunkUtils;

public class ResettableCharChunkChunk<ATTR extends Any> extends CharChunkChunk<ATTR> implements ResettableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableCharChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableCharChunkChunk<>();
    }

    private ResettableCharChunkChunk(CharChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableCharChunkChunk() {
        this(CharChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableCharChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableCharChunkChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asCharChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final CharChunk<ATTR>[] typedArray = (CharChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(CharChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(CharChunk<ATTR>[] data, int offset, int capacity) {
        ChunkUtils.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }
}
