/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ResettableWritableCharChunk and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * {@link ResettableWritableChunk} implementation for int data.
 */
public final class ResettableWritableIntChunk<ATTR_BASE extends Any> extends WritableIntChunk implements ResettableWritableChunk<ATTR_BASE> {

    public static <ATTR_BASE extends Any> ResettableWritableIntChunk<ATTR_BASE> makeResettableChunk() {
        return MultiChunkPool.forThisThread().getIntChunkPool().takeResettableWritableIntChunk();
    }

    public static <ATTR_BASE extends Any> ResettableWritableIntChunk<ATTR_BASE> makeResettableChunkForPool() {
        return new ResettableWritableIntChunk<>();
    }

    private ResettableWritableIntChunk(int[] data, int offset, int capacity) {
        super(data, offset, capacity);
    }

    private ResettableWritableIntChunk() {
        this(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    @Override
    public final ResettableWritableIntChunk slice(int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(size, offset, capacity);
        return new ResettableWritableIntChunk<>(data, this.offset + offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromChunk(WritableChunk<ATTR> other, int offset, int capacity) {
        return resetFromTypedChunk(other.asWritableIntChunk(), offset, capacity);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromArray(Object array, int offset, int capacity) {
        final int[] typedArray = (int[])array;
        return resetFromTypedArray(typedArray, offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromArray(Object array) {
        final int[] typedArray = (int[])array;
        return resetFromTypedArray(typedArray, 0, typedArray.length);
    }

    @Override
    public final <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> clear() {
        return resetFromArray(ArrayTypeUtils.EMPTY_INT_ARRAY, 0, 0);
    }

    public final <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromTypedChunk(WritableIntChunk<ATTR> other, int offset, int capacity) {
        ChunkHelpers.checkSliceArgs(other.size, offset, capacity);
        return resetFromTypedArray(other.data, other.offset + offset, capacity);
    }

    public final <ATTR extends ATTR_BASE> WritableIntChunk<ATTR> resetFromTypedArray(int[] data, int offset, int capacity) {
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
        MultiChunkPool.forThisThread().getIntChunkPool().giveResettableWritableIntChunk(this);
    }
}
