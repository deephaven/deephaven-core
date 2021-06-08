/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;
import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * {@link ResettableWritableChunk} implementation for boolean data.
 */
public final class ResettableWritableBooleanChunk<ATTR_BASE extends Any> extends WritableBooleanChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableBooleanChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getBooleanChunkPool().takeResettableWritableBooleanChunk();
    }

    public static <ATTR_BASE extends Any> ResettableWritableBooleanChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableBooleanChunk<>();
    }

    private ResettableWritableBooleanChunk(boolean[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableBooleanChunk() {
        this(ArrayUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableBooleanChunk slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableBooleanChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableBooleanChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final boolean[] typedArray = (boolean[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromArray(Object array) {
        final boolean[] typedArray = (boolean[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> clear() {
        return resetFromArray(ArrayUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromTypedChunk(WritableBooleanChunk<ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableBooleanChunk<ATTR> resetFromTypedArray(boolean[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getBooleanChunkPool().giveResettableWritableBooleanChunk(this);
    }
}
