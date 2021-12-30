/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;

public class ResettableDoubleChunkChunk<ATTR extends Any> extends DoubleChunkChunk<ATTR> implements ResettableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableDoubleChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableDoubleChunkChunk<>();
    }

    private ResettableDoubleChunkChunk(DoubleChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableDoubleChunkChunk() {
        this(DoubleChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableDoubleChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableDoubleChunkChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asDoubleChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final DoubleChunk<ATTR>[] typedArray = (DoubleChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(DoubleChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(DoubleChunk<ATTR>[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }
}
