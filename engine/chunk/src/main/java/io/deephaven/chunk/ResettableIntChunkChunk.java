/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;

public class ResettableIntChunkChunk<ATTR extends Any> extends IntChunkChunk<ATTR> implements ResettableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableIntChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableIntChunkChunk<>();
    }

    private ResettableIntChunkChunk(IntChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableIntChunkChunk() {
        this(IntChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableIntChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableIntChunkChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asIntChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final IntChunk<ATTR>[] typedArray = (IntChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(IntChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(IntChunk<ATTR>[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }
}
