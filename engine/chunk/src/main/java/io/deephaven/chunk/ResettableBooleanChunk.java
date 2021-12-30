/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * {@link ResettableReadOnlyChunk} implementation for boolean data.
 */
public final class ResettableBooleanChunk<ATTR_UPPER extends Any> extends BooleanChunk implements ResettableReadOnlyChunk<ATTR_UPPER> {

    public static <ATTR_BASE extends Any> ResettableBooleanChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getBooleanChunkPool().takeResettableBooleanChunk();
    }

    public static <ATTR_BASE extends Any> ResettableBooleanChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableBooleanChunk<>();
    }

    private ResettableBooleanChunk(boolean[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableBooleanChunk() {
        this(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);
    }

    @Override
    public final ResettableBooleanChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableBooleanChunk(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> BooleanChunk<ATTR> resetFromChunk(Chunk<? extends ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asBooleanChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> BooleanChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final boolean[] typedArray = (boolean[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> BooleanChunk<ATTR> resetFromArray(Object array) {
        final boolean[] typedArray = (boolean[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_UPPER> BooleanChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_BOOLEAN_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_UPPER> BooleanChunk<ATTR> resetFromTypedChunk(BooleanChunk<? extends ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_UPPER> BooleanChunk<ATTR> resetFromTypedArray(boolean[] data, int offset, int capacity) {
        ChunkHelpers.checkArrayArgs(data.length, offset, capacity);
        this.data = data;
        this.offset = offset;
        this.capacity = capacity;
        this.size = capacity;
        //noinspection unchecked
        return this;
    }

    @Override
    public final void close() {
        MultiChunkPool.forThisThread().getBooleanChunkPool().giveResettableBooleanChunk(this);
    }
}
