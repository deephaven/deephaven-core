/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunkChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.structures.chunk;
import io.deephaven.engine.structures.chunk.Attributes.Any;
import io.deephaven.engine.structures.chunk.ChunkUtils;

public class ResettableFloatChunkChunk<ATTR extends Any> extends FloatChunkChunk<ATTR> implements ResettableChunkChunk<ATTR> {

    public static <ATTR extends Any> ResettableFloatChunkChunk<ATTR> makeResettableChunk() {
        return new ResettableFloatChunkChunk<>();
    }

    private ResettableFloatChunkChunk(FloatChunk<ATTR>[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableFloatChunkChunk() {
        this(FloatChunk.getEmptyChunkArray(), 0, 0);
    }

    @Override
    public ResettableFloatChunkChunk<ATTR> slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableFloatChunkChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final void resetFromChunk(ChunkChunk<ATTR> other, int offset, int capacity) {
        resetFromTypedChunk(other.asFloatChunkChunk(), offset, capacity);
    }

    @Override
    public final void resetFromArray(Object array, int offset, int capacity) {
        //noinspection unchecked
        final FloatChunk<ATTR>[] typedArray = (FloatChunk<ATTR>[])array;
        resetFromTypedArray(typedArray, offset, capacity);
    }

    public final void resetFromTypedChunk(FloatChunkChunk<ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final void resetFromTypedArray(FloatChunk<ATTR>[] data, int offset, int capacity) {
        ChunkUtils.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
    }
}
