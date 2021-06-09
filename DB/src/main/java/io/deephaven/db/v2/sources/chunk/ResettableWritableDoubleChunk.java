/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;
import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * {@link ResettableWritableChunk} implementation for double data.
 */
public final class ResettableWritableDoubleChunk<ATTR_BASE extends Any> extends WritableDoubleChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableDoubleChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getDoubleChunkPool().takeResettableWritableDoubleChunk();
    }

    public static <ATTR_BASE extends Any> ResettableWritableDoubleChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableDoubleChunk<>();
    }

    private ResettableWritableDoubleChunk(double[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableDoubleChunk() {
        this(ArrayUtils.EMPTY_DOUBLE_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableDoubleChunk slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableDoubleChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableDoubleChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final double[] typedArray = (double[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromArray(Object array) {
        final double[] typedArray = (double[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> clear() {
        return resetFromArray(ArrayUtils.EMPTY_DOUBLE_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromTypedChunk(WritableDoubleChunk<ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableDoubleChunk<ATTR> resetFromTypedArray(double[] data, int offset, int capacity) {
        ChunkUtils.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        //noinspection unchecked
        return this;
    }

    @Override
    public final void close() {
        MultiChunkPool.forThisThread().getDoubleChunkPool().giveResettableWritableDoubleChunk(this);
    }
}
