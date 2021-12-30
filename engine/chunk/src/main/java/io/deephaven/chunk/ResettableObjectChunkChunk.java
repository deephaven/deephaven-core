/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;

public class ResettableObjectChunkChunk<T, ATTR extends Any> extends ObjectChunkChunk<T, ATTR> implements ResettableChunkChunk<ATTR> {

    public static <T, ATTR extends Any> ResettableObjectChunkChunk<T, ATTR> makeResettableChunk() {
        return new ResettableObjectChunkChunk<>();
    }

    private ResettableObjectChunkChunk(ObjectChunk<T, ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableObjectChunkChunk() {
        this(ObjectChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableObjectChunkChunk<T, ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableObjectChunkChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asObjectChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final ObjectChunk<T, ATTR>[] typedArray = (ObjectChunk<T, ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(ObjectChunkChunk<T, ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(ObjectChunk<T, ATTR>[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }
}
