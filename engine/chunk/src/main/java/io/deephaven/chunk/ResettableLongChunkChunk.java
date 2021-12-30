/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;

public class ResettableLongChunkChunk<ATTR extends Any> extends LongChunkChunk<ATTR> implements ResettableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableLongChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableLongChunkChunk<>();
    }

    private ResettableLongChunkChunk(LongChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableLongChunkChunk() {
        this(LongChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableLongChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableLongChunkChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asLongChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final LongChunk<ATTR>[] typedArray = (LongChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(LongChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(LongChunk<ATTR>[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }
}
