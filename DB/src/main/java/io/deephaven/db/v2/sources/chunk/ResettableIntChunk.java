/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunk and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk;
import io.deephaven.db.tables.utils.ArrayUtils;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.db.v2.utils.ChunkUtils;

/**
 * {@link ResettableReadOnlyChunk} implementation for int data.
 */
public final class ResettableIntChunk<ATTR_UPPER extends Any> extends IntChunk implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableIntChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getIntChunkPool().takeResettableIntChunk();
    }

    public static <ATTR_BASE extends Any> ResettableIntChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableIntChunk<>();
    }

    private ResettableIntChunk(int[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableIntChunk() {
        this(ArrayUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableIntChunk slice(int offset, int capacity) {
        ChunkUtils.checkSliceArgs(size, offset, capacity);
        return new ResettableIntChunk(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asIntChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final int[] typedArray = (int[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromArray(Object array) {
        final int[] typedArray = (int[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> IntChunk<ATTR> clear() {
        return resetFromArray(ArrayUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromTypedChunk(IntChunk<? extends ATTR> other, int offset, int capacity) {
        ChunkUtils.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_UPPER> IntChunk<ATTR> resetFromTypedArray(int[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getIntChunkPool().giveResettableIntChunk(this);
    }
}
