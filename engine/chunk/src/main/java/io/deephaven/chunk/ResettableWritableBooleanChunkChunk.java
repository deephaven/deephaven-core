/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunkChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;
import io.deephaven.chunk.attributes.Any;


public class ResettableWritableBooleanChunkChunk<ATTR extends Any> extends WritableBooleanChunkChunk<ATTR> implements ResettableWritableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableWritableBooleanChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableWritableBooleanChunkChunk<>();
    }

    private ResettableWritableBooleanChunkChunk(WritableBooleanChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableBooleanChunkChunk() {
        this(WritableBooleanChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableWritableBooleanChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableBooleanChunkChunk<>(writableData, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(WritableChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asWritableBooleanChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final WritableBooleanChunk<ATTR>[] typedArray = (WritableBooleanChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(WritableBooleanChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.writableData, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(WritableBooleanChunk<ATTR>[] data, int offset, int capacity) {
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
